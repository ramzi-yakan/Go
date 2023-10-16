// Ramzi Yakan   300078504

// Project CSI2120/CSI2520
// Winter 2022
// Robert Laganiere, uottawa.ca

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type GPScoord struct {
	lat  float64
	long float64
}

type LabelledGPScoord struct {
	GPScoord
	ID    int // point ID
	Label int // cluster ID
}

type Job struct {
	coords *[]LabelledGPScoord
	offset int
}

const N int = 4
const consumerThreads int = 4
const MinPts int = 5
const eps float64 = 0.0003
const filename string = "yellow_tripdata_2009-01-15_9h_21h_clean.csv"

func main() {

	start := time.Now()

	gps, minPt, maxPt := readCSVFile(filename)
	fmt.Printf("Number of points: %d\n", len(gps))

	minPt = GPScoord{40.7, -74.}
	maxPt = GPScoord{40.8, -73.93}

	// geographical limits
	fmt.Printf("SW:(%f , %f)\n", minPt.lat, minPt.long)
	fmt.Printf("NE:(%f , %f) \n\n", maxPt.lat, maxPt.long)

	// Parallel DBSCAN STEP 1.
	incx := (maxPt.long - minPt.long) / float64(N)
	incy := (maxPt.lat - minPt.lat) / float64(N)

	var grid [N][N][]LabelledGPScoord // a grid of GPScoord slices

	// Create the partition
	// triple loop! not very efficient, but easier to understand

	partitionSize := 0
	for j := 0; j < N; j++ {
		for i := 0; i < N; i++ {

			for _, pt := range gps {

				// is it inside the expanded grid cell
				if (pt.long >= minPt.long+float64(i)*incx-eps) && (pt.long < minPt.long+float64(i+1)*incx+eps) && (pt.lat >= minPt.lat+float64(j)*incy-eps) && (pt.lat < minPt.lat+float64(j+1)*incy+eps) {

					grid[i][j] = append(grid[i][j], pt) // add the point to this slide
					partitionSize++
				}
			}
		}
	}

	// ***
	// This is the non-concurrent procedural version
	// It should be replaced by a producer thread that produces jobs (partition to be clustered)
	// And by consumer threads that clusters partitions

	jobs := make(chan Job)
	var mutex sync.WaitGroup
	mutex.Add(consumerThreads)

	//produce
	go produce(jobs, grid, &mutex)

	// Parallel DBSCAN STEP 2.
	// Apply DBSCAN on each partition
	// ...

	//consume
	for i := 0; i < consumerThreads; i++ {
		go consume(jobs, &mutex)
	}

	mutex.Wait()

	// Parallel DBSCAN step 3.
	// merge clusters
	// *DO NOT PROGRAM THIS STEP

	end := time.Now()
	fmt.Printf("\nExecution time: %s of %d points\n", end.Sub(start), partitionSize)
	fmt.Printf("Number of CPUs: %d", runtime.NumCPU())
}

func produce(jobs chan Job, grid [N][N][]LabelledGPScoord, done *sync.WaitGroup) {
	for j := 0; j < N; j++ {
		for i := 0; i < N; i++ {
			nextJob := Job{&grid[i][j], i*10000000 + j*1000000}
			jobs <- nextJob
		}
	}
	close(jobs)
	done.Done()
}

func consume(jobs chan Job, done *sync.WaitGroup) {
	for {
		j, more := <-jobs

		if more {
			DBscan(j)
			time.Sleep(4 * time.Second)

		} else {
			done.Done()
			return
		}
	}
}

// Applies DBSCAN algorithm on LabelledGPScoord points
// LabelledGPScoord: the slice of LabelledGPScoord points
// MinPts, eps: parameters for the DBSCAN algorithm
// offset: label of first cluster (also used to identify the cluster)
// returns number of clusters found
func DBscan(job Job) (nclusters int) {

	addressOfCoords := job.coords
	coords := *addressOfCoords
	offset := job.offset
	nclusters = 0

	//Go through every point in our set of coordinates.
	for i := 0; i < len(coords); i++ {

		//Get the next point p
		p := coords[i]

		//Check if p has already been marked
		if p.Label != 0 {
			continue
		}

		//Find neighbours of p
		listOfNeighbours, numOfNeighbours := rangeQuery(p, addressOfCoords)

		//If the number of p's neighbours < minPts, p should be marked as noise
		if numOfNeighbours < MinPts {
			p.Label = -1
			continue
		}

		//Next cluster label
		nclusters++

		//Give p this label
		p.Label = nclusters

		//Create seedSet that includes all neighbours of p (excluding itself)
		seedSet := listOfNeighbours
		for j := 0; j < len(seedSet); j++ {
			if seedSet[j] == p {
				seedSet[j] = seedSet[len(seedSet)-1]
				seedSet = seedSet[:len(seedSet)-1]
			}
		}

		//Go through every point in our seedSet.
		for j := 0; j < len(seedSet); j++ {

			//Get the next point q
			q := seedSet[i]

			//If point was previously labelled as noise, change its label to border point
			if q.Label == -1 {
				q.Label = nclusters
			}

			//If point was previously labelled as a border point, ignore it
			if q.Label != 0 {
				continue
			}

			//Change point label to border point
			q.Label = nclusters

			//Find neighbours of q
			listOfNeighbours, numOfNeighbours = rangeQuery(q, addressOfCoords)

			//If q is a core point
			if numOfNeighbours >= MinPts {
				//Add q's neighbours to the seedSet
				seedSet = append(seedSet, listOfNeighbours...)
			}
		}
	}

	// End of DBscan function
	// Printing the result (do not remove)
	fmt.Printf("Partition %10d : [%4d,%6d]\n", offset, nclusters, len(coords))

	return nclusters
}

func rangeQuery(q LabelledGPScoord, addresses *[]LabelledGPScoord) ([]LabelledGPScoord, int) {
	coords := *addresses
	neighbours := make([]LabelledGPScoord, len(coords))
	numNeighbours := 0
	for i := 0; i < len(coords); i++ {
		p := coords[i]
		if p == q && distance(p, q) <= eps {
			neighbours[numNeighbours] = p
			numNeighbours++
		}
	}

	//fmt.Println(numNeighbours)
	return neighbours, numNeighbours
}

func distance(p LabelledGPScoord, q LabelledGPScoord) (distance float64) {
	distance = math.Sqrt(math.Pow(q.long-p.long, 2) + math.Pow(q.lat-p.lat, 2))
	return distance
}

// reads a csv file of trip records and returns a slice of the LabelledGPScoord of the pickup locations
// and the minimum and maximum GPS coordinates
func readCSVFile(filename string) (coords []LabelledGPScoord, minPt GPScoord, maxPt GPScoord) {

	coords = make([]LabelledGPScoord, 0, 5000)

	// open csv file
	src, err := os.Open(filename)
	defer src.Close()
	if err != nil {
		panic("File not found...")
	}

	// read and skip first line
	r := csv.NewReader(src)
	record, err := r.Read()
	if err != nil {
		panic("Empty file...")
	}

	minPt.long = 1000000.
	minPt.lat = 1000000.
	maxPt.long = -1000000.
	maxPt.lat = -1000000.

	var n int = 0

	for {
		// read line
		record, err = r.Read()

		// end of file?
		if err == io.EOF {
			break
		}

		if err != nil {
			panic("Invalid file format...")
		}

		// get lattitude
		lat, err := strconv.ParseFloat(record[9], 64)
		if err != nil {
			panic("Data format error (lat)...")
		}

		// is corner point?
		if lat > maxPt.lat {
			maxPt.lat = lat
		}
		if lat < minPt.lat {
			minPt.lat = lat
		}

		// get longitude
		long, err := strconv.ParseFloat(record[8], 64)
		if err != nil {
			panic("Data format error (long)...")
		}

		// is corner point?
		if long > maxPt.long {
			maxPt.long = long
		}

		if long < minPt.long {
			minPt.long = long
		}

		// add point to the slice
		n++
		pt := GPScoord{lat, long}
		coords = append(coords, LabelledGPScoord{pt, n, 0})
	}

	return coords, minPt, maxPt
}
