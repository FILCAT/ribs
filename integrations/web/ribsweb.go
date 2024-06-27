package web

import (
	"context"
	"embed"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	txtempl "text/template"

	"github.com/atboosty/ribs"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("ribsweb")

//go:embed ribswebapp/build
var dres embed.FS

type RIBSWeb struct {
	ribs ribs.RIBS
}

func (ri *RIBSWeb) Index(w http.ResponseWriter, r *http.Request) {
	// todo there has to be something better than this horrid hack
	_, err := dres.ReadFile(filepath.Join("ribswebapp", "build", r.URL.Path))
	if err == nil && r.URL.Path != "/" {
		r.URL.Path = filepath.Join("ribswebapp", "build", r.URL.Path)
		http.FileServer(http.FS(dres)).ServeHTTP(w, r)
		return
	}

	log.Errorw("index", "path", r.URL.Path)

	tpl, err := txtempl.New("index.html").ParseFS(dres, "ribswebapp/build/index.html")
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

func Serve(ctx context.Context, listen string, ribs ribs.RIBS) error {
	handlers := &RIBSWeb{
		ribs: ribs,
	}

	rpc, closer, err := MakeRPCServer(ctx, ribs)
	if err != nil {
		return err
	}
	defer closer()

	mux := http.NewServeMux()
	mux.HandleFunc("/", handlers.Index)

	mux.Handle("/rpc/v0", rpc)

	mux.Handle("/debug/", http.DefaultServeMux)

	server := &http.Server{Addr: listen, Handler: mux, BaseContext: func(_ net.Listener) context.Context { return ctx }}
	return server.ListenAndServe()
}
