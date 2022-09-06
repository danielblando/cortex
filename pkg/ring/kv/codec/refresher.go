package codec

type Refresher interface {
	//RefreshTimestamp updates model timestamp.
	RefreshTimestamp()

	// Clone should return a deep copy of the state.
	Clone() interface{}
}
