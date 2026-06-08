package cli

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func isGitcrawlShimPath(path string) bool {
	if path == "" {
		return false
	}
	resolved := path
	if eval, err := filepath.EvalSymlinks(path); err == nil {
		resolved = eval
	}
	for _, value := range []string{path, resolved} {
		base := strings.ToLower(filepath.Base(value))
		if base == "gitcrawl" || base == "gitcrawl-gh" {
			return true
		}
	}
	return false
}

func usableRealGHPath(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() || !ghBackendModeUsable(info.Mode(), runtime.GOOS) {
		return false
	}
	exe, err := os.Executable()
	if err != nil {
		return true
	}
	exeInfo, exeInfoErr := os.Stat(exe)
	if exeInfoErr == nil && os.SameFile(info, exeInfo) {
		return false
	}
	candidateReal, candidateErr := filepath.EvalSymlinks(path)
	exeReal, exeErr := filepath.EvalSymlinks(exe)
	if candidateErr == nil && exeErr == nil && candidateReal == exeReal {
		return false
	}
	return !sameExecutableContents(path, exe, info, exeInfo)
}

func ghBackendModeUsable(mode os.FileMode, goos string) bool {
	return goos == "windows" || mode&0111 != 0
}

func sameExecutableContents(candidate, exe string, candidateInfo, exeInfo os.FileInfo) bool {
	if candidateInfo == nil || exeInfo == nil || candidateInfo.Size() != exeInfo.Size() {
		return false
	}
	candidateHash, err := fileSHA256(candidate)
	if err != nil {
		return false
	}
	exeHash, err := fileSHA256(exe)
	if err != nil {
		return false
	}
	return candidateHash == exeHash
}

func fileSHA256(path string) ([32]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return [32]byte{}, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return [32]byte{}, err
	}
	var out [32]byte
	copy(out[:], hash.Sum(nil))
	return out, nil
}
