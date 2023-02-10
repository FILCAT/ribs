package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"github.com/lotus-web3/ribs"
	"net/http"
	"sort"
	"strconv"
	txtempl "text/template"
)

//go:embed static
var dres embed.FS

type RIBSWeb struct {
	ribs ribs.RIBS
}

func (ri *RIBSWeb) Index(w http.ResponseWriter, r *http.Request) {
	tpl, err := txtempl.New("index.html").ParseFS(dres, "static/index.html")
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	data := map[string]interface{}{}
	if err := tpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

func (ri *RIBSWeb) ApiGroups(w http.ResponseWriter, r *http.Request) {
	gs, err := ri.ribs.Diagnostics().Groups()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	sort.Slice(gs, func(i, j int) bool {
		return gs[i] < gs[j]
	})

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(gs); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

func (ri *RIBSWeb) ApiGroup(w http.ResponseWriter, r *http.Request) {
	grp := r.FormValue("group")
	if grp == "" {
		http.Error(w, "missing group", 400)
		return
	}
	gint, err := strconv.ParseUint(grp, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	gm, err := ri.ribs.Diagnostics().GroupMeta(ribs.GroupKey(gint))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(gm); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

func Serve(listen string, ribs ribs.RIBS) error {
	handlers := &RIBSWeb{
		ribs: ribs,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", handlers.Index)

	mux.HandleFunc("/api/v0/groups", handlers.ApiGroups)
	mux.HandleFunc("/api/v0/group", handlers.ApiGroup)

	return http.ListenAndServe(listen, mux)
}
