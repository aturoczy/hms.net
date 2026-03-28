// This file is intentionally empty — connection handling and handler creation
// are managed directly by ThriftMetastoreServer.HandleClientAsync using DI scopes.
// The factory pattern is embedded in the server since we use raw TCP rather than
// the Apache.Thrift library's server abstractions.
