package metric

type ErrorQueryConditionExceedMaximum struct {
	message string
}

func NewErrorQueryConditionExceedMaximum(message string) ErrorQueryConditionExceedMaximum {
	return ErrorQueryConditionExceedMaximum{
		message: message,
	}
}

func (e ErrorQueryConditionExceedMaximum) Error() string {
	return e.message
}
