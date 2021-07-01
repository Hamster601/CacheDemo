package server

import (
	"net/http"

	cache "github.com/Hamster601/CacheDemo/internal"
)

//服务端实现
type Server struct {
	cache.Cache
	cache.Node
}

func (s *Server) Listen() {
	http.Handle("/cache/", s.cacheHandler())
	http.Handle("/status", s.statusHandler())
	http.Handle("/cluster", s.clusterHandler())
	http.Handle("/rebalance", s.rebalanceHandler())
	http.ListenAndServe(s.Addr()+":12345", nil)
}

func New(c cache.Cache, n cache.Node) *Server {
	return &Server{c, n}
}
