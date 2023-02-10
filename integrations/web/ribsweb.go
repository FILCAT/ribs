package web

import (
	"github.com/lotus-web3/ribs"
	"net/http"
)

type RIBSWeb struct {
	ribs ribs.RIBS
}

func (ri *RIBSWeb) Index(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("Hello World!"))
}

func Serve(listen string, ribs ribs.RIBS) error {
	handlers := &RIBSWeb{
		ribs: ribs,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", handlers.Index)

	return http.ListenAndServe(listen, mux)
}

