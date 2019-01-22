package metric

import ()

type ErrorQueryConditionExceedMaximum struct {
	message string
	err     error
}

func NewErrorQueryConditionExceedMaximum(message string, e error) ErrorQueryConditionExceedMaximum {
	return ErrorQueryConditionExceedMaximum{
		message: message,
		err:     e,
	}
}

func (e ErrorQueryConditionExceedMaximum) Error() string {
	return e.message
}

func (e ErrorQueryConditionExceedMaximum) Cause() error {
	return e.err
}
