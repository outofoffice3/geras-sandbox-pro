package geras

type gqError struct {
	message string
}

func (err *gqError) Error() string {
	return err.message
}

func newError(errMsg string) error {
	return &gqError{
		message: errMsg,
	}
}
