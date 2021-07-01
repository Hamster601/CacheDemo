package tcp

import (
	"net"

	cache "github.com/Hamster601/CacheDemo/internal/"
)

type Server struct {
	cache.Cache
	cache.Node
}

func (s *Server) Listen() {
	//监听tcp端口
	l, e := net.Listen("tcp", ":12346")
	if e != nil {
		panic(e)
	}
	for {
		c, e := l.Accept()
		if e != nil {
			panic(e)
		}
		go s.process(c)
	}
}

func New(c cache.Cache, n cache.Node) *Server {
	return &Server{c, n}
}
