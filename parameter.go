package wire

func NewParameter(format FormatCode, value []byte) Parameter {
	return Parameter{
		format: format,
		value:  value,
	}
}

type Parameter struct {
	format FormatCode
	value  []byte
}

func (p Parameter) Format() FormatCode {
	return p.format
}

func (p Parameter) Value() []byte {
	return p.value
}
