package main

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
)

type NameAndSize struct {
	Name string
	Size int64
}

type NameAndSizes []NameAndSize

type Plan struct {
	Pool   NameAndSizes
	Writer *tar.Writer
}

func main() {
	data, err := generateSlice("./knowledge-base1.tar")
	if err != nil {
		log.Fatal(err)
	}
	sort.Sort(sort.Reverse(data))
	targetSize := int64(8600000000)
	plans := buildTarPlan(data, targetSize)
	err = createNewTars("./knowledge-base1.tar", &plans)
	if err != nil {
		log.Fatal(err)
	}
}

func generateSlice(filename string) (NameAndSizes, error) {

	var tarreader io.Reader

	filereader, err := os.Open(filename)
	if err != nil {
		return NameAndSizes{}, err
	}
	defer filereader.Close()

	if filepath.Ext(filename) == ".gz" {
		tarreader, err := gzip.NewReader(filereader)
		if err != nil {
			return NameAndSizes{}, err
		}
		defer tarreader.Close()
	} else {
		tarreader = filereader
	}

	tr := tar.NewReader(tarreader)
	info := make(NameAndSizes, 0)

	for {
		header, err := tr.Next()
		switch {
		case err == io.EOF:
			return info, nil

		case err != nil:
			return NameAndSizes{}, err

		case header == nil:
			continue
		}
		fi := header.FileInfo()
		info = append(info, NameAndSize{header.Name, fi.Size()})
	}
}

func buildTarPlan(data NameAndSizes, targetSize int64) []Plan {
	//Since I can't think of any other way, going to start with the biggest and once
	//the next biggest can't fit, going to top it off with the bottom up till we get all
	plans := make([]Plan, 0)

	var currentPlanTotalSize int64
	currentPlan := &Plan{}
	endIndex := len(data) - 1
	finished := false
	addToNext := false
	canAddSmall := true

	for i := 0; i <= endIndex; i++ {
		if currentPlanTotalSize+data[i].Size <= targetSize {
			currentPlan.Pool = append(currentPlan.Pool, data[i])
			currentPlanTotalSize = currentPlanTotalSize + data[i].Size
		} else {
			//Time to fill up from reverse
			for endIndex >= i {
				if currentPlanTotalSize+data[endIndex].Size < targetSize {
					currentPlan.Pool = append(currentPlan.Pool, data[endIndex])
					currentPlanTotalSize += data[endIndex].Size
					endIndex = endIndex - 1
				} else {
					canAddSmall = false
					break
				}
			}
			addToNext = true
		}
		if i == endIndex {
			finished = true
		}

		if finished || !canAddSmall {
			plans = append(plans, *currentPlan)
			//Need a new plan to add to and reset counters
			currentPlan = &Plan{}
			currentPlanTotalSize = 0
			canAddSmall = true
			if addToNext {
				currentPlan.Pool = append(currentPlan.Pool, data[i])
				currentPlanTotalSize += data[i].Size
				addToNext = false
				if finished {
					plans = append(plans, *currentPlan)
				}
			}
		}
		if finished {
			break
		}
	}
	return plans
}

func createNewTars(filename string, plans *[]Plan) error {

	var genericReader io.Reader
	//Create a map to define pointer for each name
	filenamePtrMap := make(map[string]*tar.Writer)

	osReader, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer osReader.Close()

	if filepath.Ext(filename) == ".gz" {
		tarReader, err := gzip.NewReader(osReader)
		if err != nil {
			return err
		}
		defer tarReader.Close()
	} else {
		genericReader = osReader
	}

	tarReader := tar.NewReader(genericReader)

	for i, plan := range *plans {
		file, err := os.Create(fmt.Sprintf("./knowledge-test-%v.tar", i))
		if err != nil {
			return fmt.Errorf("Could not create tarball file '%s', got error '%s'", "./knowledge-test-%s.tar", err.Error())
		}
		defer file.Close()
		tw := tar.NewWriter(file)
		defer tw.Close()
		for _, fn := range plan.Pool {
			filenamePtrMap[fn.Name] = tw
		}
	}

	for {
		header, err := tarReader.Next()
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		case header == nil:
			continue
		}
		switch header.Typeflag {
		case tar.TypeReg:
			mw := filenamePtrMap[header.Name]
			if mw != nil {
				if err := mw.WriteHeader(header); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("Missing writer ptr for file %s", header.Name)
			}
			if _, err := io.Copy(mw, tarReader); err != nil {
				return err
			}
		}
	}
}

func (s NameAndSizes) Len() int {
	return len(s)
}

func (s NameAndSizes) Less(i, j int) bool {
	return s[i].Size < s[j].Size
}

func (s NameAndSizes) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
