package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

    "github.com/brugger/probes-rest-api/internal/db"

	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
)

var version string = "0.0.0"

var sqlite_file = "probes.db"

func checkErr(err error) {
    if err != nil {
        log.Fatalln("Error")
        log.Fatal(err)
    }
}

func dbGetProbes(filter map[string]string) ([]Probe) {
    fmt.Println( filter )

    // insert
    stmt := "SELECT * FROM probes"

    var conds []string
    for key, value := range filter {
        switch key {
            case "from":
                conds = append(conds, fmt.Sprintf(" pos_%s_vcf >= '%s'", filter["coords"], value))
            case "to":
                conds = append(conds, fmt.Sprintf(" pos_%s_vcf <= '%s'", filter["coords"], value))
            case "coords":
            default:
                conds = append( conds, fmt.Sprintf(" %s = '%s'", key, value))
        }
    }

    if len( conds) > 0 {
        stmt = fmt.Sprintf("%s WHERE %s ", stmt, strings.Join(conds[:], " AND "))
    }

    fmt.Println( stmt )

    db, err := sql.Open("sqlite3", sqlite_file)
    checkErr(err)

    rows, err := db.Query(stmt)
    checkErr(err)
    var probes []Probe

    for rows.Next() {

        var new_probe Probe



        err = rows.Scan(&new_probe.Probeset_id,
                        &new_probe.Affy_snp_id,
                        &new_probe.Rsid,
                        &new_probe.Chr,
                        &new_probe.HG19_pos,
                        &new_probe.HG19_ref,
                        &new_probe.HG19_alt,
                        &new_probe.HG38_pos,
                        &new_probe.HG38_ref,
                        &new_probe.HG38_alt,
                        &new_probe.Gene,
                        &new_probe.Cpos,
                        &new_probe.Source,
                        &new_probe.Category)

        probes = append( probes,  new_probe)
        //checkErr(err)
    }

    rows.Close() //good habit to close

    db.Close()

//    fmt.Println( probes )
    return probes
}


type Probe struct {
    Probeset_id string `json:"probeset_id"`
    Affy_snp_id int    `json:"affy_snp_id"`
    Rsid        sql.NullString `json:"rsid"`
    Chr         string `json:"chr"`
    HG19_pos    int    `json:"hg19_pos"`
    HG19_ref    string `json:"hg19_ref"`
    HG19_alt    string `json:"hg19_alt"`
    HG38_pos    int    `json:"hg38_pos"`
    HG38_ref    string `json:"hg38_ref"`
    HG38_alt    string `json:"hg38_alt"`
    Gene        string `json:"gene"`
    Cpos        string `json:"c_pos"`

    Source      string `json:"source"`
    Category    string `json:"category"`
}



func infoPage(w http.ResponseWriter, r *http.Request){
    fmt.Println("Endpoint Hit: infoPage")
    info := map[string]string{"name":"array-api", "version":version}


    json.NewEncoder(w).Encode(info)

}


func getProbes(w http.ResponseWriter, r *http.Request){
    fmt.Println("Endpoint Hit: probes")
    query := r.URL.Query()

    values := make(map[string]string)
    for key, _ := range query {
        values[key] = query.Get(key)
    }

    if _, ok := values["coords"]; !ok {
        values["coords"] = "hg38"
    }


    if _, ok := values["pos"]; ok {
        fmt.Println("pos provided")

        s := strings.Split(values["pos"], "-")
        fmt.Println( "S" + strings.Join(s, ",") )
        if len(s) > 1 {
            values["from"] = s[0]
            values["to"] = s[1]
        } else if _, ok := values["coords"]; ok {
            switch values["coords"] {
            case "hg19":
                values[ "pos_hg19_vcf" ] = values["pos"]
            case "hg38":
                values[ "pos_hg38_vcf" ] = values["pos"]
            default:
                err := fmt.Sprintf("Unknown coord system '%s'\n", values["coords"])
                http.Error(w, err, http.StatusBadRequest)
                return

            }
        }
        delete( values, "pos")
    }



    probes := dbGetProbes( values )
    json.NewEncoder(w).Encode( probes )
}



func readProbes(filename string) ([]Probe){

    fmt.Println( "reading probes" )

    csvfile, err := os.Open(filename)
    if err != nil {
    	log.Fatalln("Couldn't open the csv file", err)
    }

    var records []Probe
    cvsReader := csv.NewReader(csvfile)
    cvsReader.Comma = '\t' // Use tab-delimited instead of comma

    for {
        row, err := cvsReader.Read()
        if err != nil {
            if err == io.EOF {
                err = nil
            }
//            fmt.Println( records )
            return records
        }

        var probe Probe
        probe.Probeset_id = row[ 0  ]
        probe.Affy_snp_id, err = strconv.Atoi(row[ 1  ])
//        probe.Rsid        = row[ 2  ]
        probe.Chr         = row[ 3  ]
        probe.HG19_pos, err    = strconv.Atoi(row[ 4  ])
        probe.HG19_ref    = row[ 5  ]
        probe.HG19_alt    = row[ 6  ]
        probe.HG38_pos, err    = strconv.Atoi(row[ 7  ])
        probe.HG38_ref    = row[ 8  ]
        probe.HG38_alt    = row[ 9  ]
        probe.Source      = row[ 10 ]
        probe.Category    = row[ 11 ]

//        fmt.Println( probe )

        records = append( records, probe )
    }

}



func handleRequests() {
    myRouter := mux.NewRouter().StrictSlash(true)
    myRouter.HandleFunc("/", infoPage)
    myRouter.HandleFunc("/probes/", getProbes)
//    myRouter.HandleFunc("/probe/{id}", createProbe).Methods("POST")
//    myRouter.HandleFunc("/probe/{id}/", readProbes)
//    myRouter.HandleFunc("/probe/{id}", updateProbe).Methods("PATCH")
//    myRouter.HandleFunc("/probe/{id}", deleteProbe).Methods("DELETE")
    var port = 10000
    var portString = fmt.Sprintf(":%d" , port)
    fmt.Println("Listening on port" , portString)
    log.Fatal(http.ListenAndServe( portString , myRouter))
    
}
func main() {
//    probes = readProbes("example.array.design.tsv")
//    fmt.Println( probes[0] )

//    json.NewEncoder(os.Stdout).Encode( probes[0] )
    db.hello_world()
    handleRequests()

}
