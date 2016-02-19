package main

import (
	"errors"
	"fmt"
	"os"
)

func parseCommand(args []string) (command, error) {
	switch args[0] {
	case "desc-connections":
		return descConnectionsCmd{}, nil
	case "disconnect":
		return parseDisconnectCmd(args[1:])
	case "connect-cassandra":
		return parseConnectCassandraCmd(args[1:])
	case "exit":
		return exitCmd{}, nil
	case "help":
		return helpCmd{}, nil
	default:
		return nil, errors.New("Cmd not found")
	}
}

type command interface {
	execute()
}

//desc-connections
type descConnectionsCmd struct {}
func (descConnectionsCmd) execute() {
	fmt.Println("desc-connections unimplemented")
}

//disconnect
type disconnectCmd struct {
	connectiondID string
}

func parseDisconnectCmd(args []string) (*disconnectCmd, error) {
	if len(args) != 1 {
		return nil, errors.New("Expecting 1 argument for disconnect command.")
	}

	return &disconnectCmd{args[0]}, nil
}

func (disconnectCmd) execute() {
	fmt.Println("disconnect unimplemented")
}

//connect-cassandra
type connectCassandraCmd struct {
	username string
	password string
	ips      []string
}

func parseConnectCassandraCmd(args []string) (*connectCassandraCmd, error) {
	if len(args) < 3 {
		return nil, errors.New("Expecting at least 3 arguments for connect-cassandra command.")
	}

	return &connectCassandraCmd{args[0], args[1], args[2:]}, nil
}

func (connectCassandraCmd) execute() {
	fmt.Println("connect-cassandra unimplemented")
}

//exit
type exitCmd struct {}
func (exitCmd) execute() {
	os.Exit(0)
}

//help
type helpCmd struct {}
func (helpCmd) execute() {
	fmt.Println("help unimplemented")
}
