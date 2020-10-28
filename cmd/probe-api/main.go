package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"github.com/brugger/kbr-go-tools/db"

	"github.com/gorilla/mux"
)


var version string = "0.0.0"

var sqlite_file = "probes.db"

func checkErr(err error) {
    if err != nil {
        log.Fatalln("Error")
        log.Fatal(err)
    }
}

func dbGetProbes(filter map[string]string) ([]map[string]interface{}) {
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

    dbUtils.Connect( "sqlite3", sqlite_file )
    rows := dbUtils.AsList( stmt )
    dbUtils.Close()
    return rows
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
    fmt.Println( probes )
    json.NewEncoder(w).Encode( probes )
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
    handleRequests()
}
