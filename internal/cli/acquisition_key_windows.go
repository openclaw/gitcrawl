package cli

import "errors"

func readAcquisitionRootKey(string) ([]byte, error) {
	return nil, errors.New("rooted acquisition token minting is supported on Unix only")
}
