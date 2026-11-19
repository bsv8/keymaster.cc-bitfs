package output

import "context"

// Publisher exports verified download content. An adapter reads staged
// content from the download repository and publishes to the host's destination.
type Publisher interface {
	Final() (*FinalFile, error)
	Complete(context.Context, string, string) (*Result, error)
	RenameFinal(string) (*FinalFile, error)
}
