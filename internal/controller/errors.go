package controller

type controllerError struct {
	message string
}

func (err *controllerError) Error() string {
	return err.message
}

func newError(errMsg string) error {
	return &controllerError{
		message: errMsg,
	}
}
