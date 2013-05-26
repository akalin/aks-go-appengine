package aksprimalitytester

import "appengine"
import "fmt"
import "github.com/akalin/aks-go/aks"
import "log"
import "math/big"
import "net/http"
import "runtime"

func init() {
	http.HandleFunc("/getAKSWitness", getAKSWitnessHandler)
}

func emitError(c appengine.Context, w http.ResponseWriter, error string) {
	c.Errorf("%s", error)
	http.Error(w, error, http.StatusInternalServerError)
}

func getAKSWitnessHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	parseFormBigInt := func(key string) *big.Int {
		str := r.FormValue(key)
		var i big.Int
		if _, parsed := i.SetString(str, 10); !parsed {
			emitError(c, w, "Could not parse "+key+"="+str)
			return nil
		}
		return &i
	}

	n := parseFormBigInt("n")
	if n == nil {
		return
	}

	R := parseFormBigInt("r")
	if R == nil {
		return
	}

	start := parseFormBigInt("start")
	if start == nil {
		return
	}

	end := parseFormBigInt("end")
	if end == nil {
		return
	}

	jobs := runtime.NumCPU()

	logger := log.New(w, "", 0)
	a := aks.GetAKSWitness(n, R, start, end, jobs, logger)
	if a != nil {
		fmt.Fprintf(w, "%v has AKS witness %v\n", n, a)
	} else {
		fmt.Fprintf(
			w, "%v has no AKS witness in [%v, %v)\n",
			n, start, end)
	}
}
