// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import (
	"DS_PA1/rpcs"
	"net"
	"bufio"
	"strconv"
	"net/http"
	"net/rpc"
)

type keyValueServer struct {
	put_value_by_key chan rpcs.PutArgs
	get_by_key chan string
	get_by_keyX chan []byte


	listener net.Listener
	clientList map[int] *client
	connectedClients int
	addNewClient chan net.Conn
	closeServer chan bool
	countYesNo chan bool
	sendingCount chan int
	deleteClient chan *client
	msgFromClient chan *node
	temp chan bool
}

type node struct {
	msg string
	id int
}

type client struct {
	id int
	conn net.Conn
	kvs *keyValueServer
	responseFromServer chan string
	stopWriting chan bool
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	server := &keyValueServer{
		clientList:         make(map[int]*client),
		connectedClients: -1,
		addNewClient:      make(chan net.Conn),
		closeServer:     make(chan bool),
		countYesNo:    make(chan bool),
		sendingCount:   make(chan int),
		deleteClient:           make(chan *client),
		msgFromClient:              make(chan *node),
		put_value_by_key: make(chan rpcs.PutArgs),
		get_by_key: make(chan string),
		get_by_keyX: make(chan []byte)}

	initDB()
	return server
}

func (kvs *keyValueServer) StartModel1(port int) error {
	ln, err := net.Listen("tcp", ":" + strconv.Itoa(port))
	kvs.listener = ln
	if err != nil {
		return nil
	}

	go serverHandlingClients(kvs)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			kvs.addNewClient <- conn
		}
	}()
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.closeServer <- true
}

func (kvs *keyValueServer) Count() int {
	kvs.countYesNo <- true
	return <- kvs.sendingCount
}

func (kvs *keyValueServer) StartModel2(port int) error {
	done := make(chan error)
	go func() {
		ln, err := net.Listen("tcp", ":" + strconv.Itoa(port))
		kvs.listener = ln
		if err != nil {
			done <- err
			return
		}
		done <- nil
		rpcServer := rpc.NewServer()
		rpcServer.Register(rpcs.Wrap(kvs))
		http.DefaultServeMux = http.NewServeMux()
		rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
		go db2(kvs)
		go http.Serve(ln, nil)

		<- kvs.closeServer
		ln.Close()
	}()

	e := <- done
	return e
}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {
	kvs.get_by_key<- args.Key
	ans := <-kvs.get_by_keyX
	reply.Value = ans
	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {
	kvs.put_value_by_key <- *args
	return nil
}

func db2(kvs* keyValueServer){
	for{
		select{
		case k := <-kvs.get_by_key:
			ans:= get(k)
			kvs.get_by_keyX <- ans
		case p := <-kvs.put_value_by_key:
			put(p.Key, p.Value)
		}
	}
}
// TODO: add additional methods/functions below!
func serverHandlingClients(kvs *keyValueServer) {
	for {
		select {
		// case to add a new client to map
		case conn := <-kvs.addNewClient:
			newClientHandler(conn,kvs)
		// read message from client
		case message := <- kvs.msgFromClient:
			msgFromClientHandler(message,kvs)
		// remove the client because client is not online anymore
		case newClient := <- kvs.deleteClient:
			newClient.stopWriting <- true
			delete(kvs.clientList,newClient.id)
		// sends total count back to Count()
		case <- kvs.countYesNo:
			length := len(kvs.clientList)
			kvs.sendingCount <- length
		// close whole server
		case <- kvs.closeServer:
			for _,client := range kvs.clientList {
				client.conn.Close()
				client.stopWriting <-true
			}
			kvs.listener.Close()
			return 
		}
	}
}

func newClientHandler(conn net.Conn,kvs *keyValueServer) {
	newId := kvs.connectedClients + 1
	kvs.connectedClients = newId

	newClient := &client{
		id: kvs.connectedClients,
		conn: conn,
		kvs: kvs,
		responseFromServer: make(chan string,500),
		stopWriting: make(chan bool)}
	go clientReadHandler(newClient)
	go clientWriteHandler(newClient)
	_, ok := kvs.clientList[newId]
	if !ok {
		kvs.clientList[newId] = newClient
	}
}

func msgFromClientHandler(message *node, kvs *keyValueServer) {
	command,key,value := parsingData(message.msg)
	if command == "put" {
		put(key,[]byte(value)) // write command
		// fmt.Println("Value Put -> Key: ",key," Value: ",string(value))
	}
	if command == "get" {
		v := get(key)
		// fmt.Println("Value Get -> Key: ",key, "Value: ",string(v))
		finalText := key + "," + string(v)
		
		for _, client := range kvs.clientList {
			select {
			case client.responseFromServer <- finalText:
			default:
				break
			}
			// client.conn.Write([]byte(finalText))			
		}
	}
}



func clientReadHandler(newClient *client) {
	defer Clean(newClient.conn)

	rw := ConnectionToRW(newClient.conn)
	for {
		msg, err := rw.ReadString('\n') 
		if err != nil {
			// close socket as well
			newClient.conn.Close()
			newClient.kvs.deleteClient <- newClient
			return
		}
		id := newClient.id
		message := & node{
			msg: msg,
			id: id}
		newClient.kvs.msgFromClient <- message
	}
}

func clientWriteHandler(newClient *client) {
	defer Clean(newClient.conn)

	rw := ConnectionToRW(newClient.conn)
	for {
		select {
		case <- newClient.stopWriting:
			return
		case data := <- newClient.responseFromServer:
			rw.WriteString(data)
			rw.Flush()
		}
	}
}

// Clean closes a connection
func Clean(conn net.Conn) {
	// clean up connection related data structures and goroutines here
	conn.Close()
}

// ConnectionToRW takes a connection and returns a buffered reader / writer on it
func ConnectionToRW(conn net.Conn) *bufio.ReadWriter {
	return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
}

func check(word string) int  {
	for index,letter := range word {
		if letter == 44 {
			return index
		}
	}
	return -1
}


func parsingData(msg string) (string,string,[]byte) {
	
	var command,key,value string

	for i:= 0; i <3 ; i ++ {
		spaceIndex := check(msg)
		if command == "get" {
			key = msg[:len(msg)-1]
			break
		} else if i == 0 {
			command = msg[:spaceIndex]
		} else if i == 1 {
			key = msg[:spaceIndex]
		} else {
			value = msg
		}
		msg = msg[spaceIndex+1:]
	}
	return command,key,[]byte(value)
}

