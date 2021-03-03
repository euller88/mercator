package main

import (
	"archive/zip"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/clbanning/mxj/v2"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

type PointData struct {
	Coordinates string
	Description string
	Name        string
}

const (
	coordinatesPath = "kml.Document.Placemark.Point.coordinates"
	descriptionPath = "kml.Document.Placemark.description"
	namePath        = "kml.Document.Placemark.name"
)

func main() {
	var arg string

	if len(os.Args) < 2 {
		arg = "."
	} else {
		arg = os.Args[1]
	}

	paths, err := crawlFileSystem(arg)
	if err != nil {
		log.Fatal(err)
	}

	var (
		numCpus                = runtime.NumCPU()
		numPathsToProcess      = len(paths)
		pathsToProcess         = make(chan string, numPathsToProcess)
		processedPaths         = make(chan *PointData, numPathsToProcess)
		dataFromProcessedPaths = make([]*PointData, numPathsToProcess)
		worker                 int
	)

	for worker = 1; worker <= numCpus; worker++ {
		go processPaths(worker, pathsToProcess, processedPaths)
	}

	for j := 0; j < numPathsToProcess; j++ {
		pathsToProcess <- paths[j]
	}
	close(pathsToProcess)

	for a := 0; a < numPathsToProcess; a++ {
		dataFromProcessedPaths[a] = <-processedPaths
	}

	fmt.Println(dataFromProcessedPaths)

	databaseFilename := uuid.NewString() + ".db"

	file, err := os.Create(databaseFilename)
	if err != nil {
		log.Fatal(err)
	}
	file.Close()

	db, err := sql.Open("sqlite3", databaseFilename)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = createTableInSQLite(db)
	if err != nil {
		log.Fatal(err)
	}

	for _, pd := range dataFromProcessedPaths {
		err = pointDataToSQLite(pd, db)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func createTableInSQLite(db *sql.DB) error {
	createTableSql := `create table pontos(
		id text not null primary key,
		nome text,
		descricao text,
		latitude real,
		longitude real
	);`

	statement, err := db.Prepare(createTableSql)
	if err != nil {
		return err
	}
	_, err = statement.Exec()
	return err
}

func pointDataToSQLite(pd *PointData, db *sql.DB) error {
	if pd == nil {
		return errors.New("dados do ponto vieram nulos")
	}

	insertPoint := `
		insert into pontos(id, nome, descricao, latitude, longitude)
		values (?, ?, ?, ?, ?)
	`
	statement, err := db.Prepare(insertPoint)
	if err != nil {
		return err
	}
	u, _ := uuid.NewRandom()

	d := strings.Split(pd.Coordinates, ",")

	longitude, _ := strconv.ParseFloat(d[0], 64)
	latitude, _ := strconv.ParseFloat(d[1], 64)

	_, err = statement.Exec(u.String(), pd.Name, pd.Description, longitude, latitude)
	return err
}

func crawlFileSystem(initialPath string) ([]string, error) {
	var paths []string
	err := filepath.Walk(
		initialPath,
		func(pathSlice *[]string) filepath.WalkFunc {
			return func(path string, info fs.FileInfo, err error) error {
				if err != nil {
					return err
				}

				if strings.HasSuffix(path, ".kmz") {
					*pathSlice = append(*pathSlice, path)
				}

				return nil
			}
		}(&paths),
	)
	return paths, err
}

func processPaths(id int, pathToProcess <-chan string, processedPaths chan<- *PointData) {
	for j := range pathToProcess {
		fmt.Println("worker", id, "processando caminho", j)
		result, _ := unzip(j)
		fmt.Println("worker", id, "processou caminho", j)
		processedPaths <- result
	}
}

func decodeXmlToMap(src *zip.File) (*PointData, error) {
	file, err := src.Open()
	if err != nil {
		return nil, err
	}
	defer file.Close()

	xmlTree, err := mxj.NewMapXmlReader(file)
	if err != nil {
		return nil, err
	}

	coordinates, err := xmlTree.ValueForPath(coordinatesPath)
	if err != nil {
		return nil, err
	}
	description, err := xmlTree.ValueForPath(descriptionPath)
	if err != nil {
		return nil, err
	}
	name, err := xmlTree.ValueForPath(namePath)
	if err != nil {
		return nil, err
	}

	return &PointData{
		Coordinates: coordinates.(string),
		Description: description.(string),
		Name:        name.(string),
	}, nil
}

func unzip(src string) (*PointData, error) {
	r, err := zip.OpenReader(src)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var kml PointData

	for _, file := range r.File {
		if strings.HasSuffix(file.Name, ".kml") {
			pointData, err := decodeXmlToMap(file)
			if err != nil {
				log.Println(err)
				break
			}
			kml = *pointData
			break
		}
	}

	return &kml, nil
}
