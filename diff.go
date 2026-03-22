package repomofo

import (
	"errors"
	"io"
	"strings"
	"unicode/utf8"
)

const maxLineSize = 10000
const maxLineCount = 100000

var errStreamTooLong = errors.New("stream too long")

// ---------------------------------------------------------------------------
// LineIterator
// ---------------------------------------------------------------------------

// lineSource is the interface for different line-reading backends.
type lineSource interface {
	// readLine reads the next line (without trailing newline).
	// Returns ("", false, nil) at EOF.
	readLine() (string, bool, error)
	// resetSource resets the source to the beginning.
	resetSource()
	// seekToLine seeks to the given line number at the given byte offset.
	seekToLine(lineNum int, offset uint64)
	// closeSource releases resources.
	closeSource()
	// isBinary returns true if the source was determined to be binary.
	isBinary() bool
	// setBinary marks the source as binary.
	setBinary()
}

// LineIterator reads lines from a source, tracking line offsets for seeking.
type LineIterator struct {
	source      lineSource
	lineOffsets []uint64
	currentLine int
}

func (it *LineIterator) count() int {
	return len(it.lineOffsets)
}

func (it *LineIterator) next() (string, bool, error) {
	line, ok, err := it.source.readLine()
	if ok {
		it.currentLine++
	}
	return line, ok, err
}

func (it *LineIterator) get(lineNum int) (string, error) {
	it.seekTo(lineNum)
	line, ok, err := it.next()
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errors.New("expected line")
	}
	return line, nil
}

func (it *LineIterator) seekTo(lineNum int) {
	if lineNum == it.currentLine {
		return
	}
	var offset uint64
	if lineNum < len(it.lineOffsets) {
		offset = it.lineOffsets[lineNum]
	}
	it.source.seekToLine(lineNum, offset)
	it.currentLine = lineNum
}

func (it *LineIterator) reset() {
	it.currentLine = 0
	it.source.resetSource()
}

func (it *LineIterator) close() {
	it.source.closeSource()
}

// validateLines reads each line to populate lineOffsets and detect binary data.
func (it *LineIterator) validateLines() error {
	var offsets []uint64
	var lastPos uint64

	for {
		line, ok, err := it.next()
		if err != nil {
			if errors.Is(err, errStreamTooLong) {
				it.source.setBinary()
				it.lineOffsets = nil
				return nil
			}
			return err
		}
		if !ok {
			break
		}

		if !utf8.ValidString(line) || len(offsets) >= maxLineCount {
			it.source.setBinary()
			it.lineOffsets = nil
			return nil
		}

		offsets = append(offsets, lastPos)
		lastPos += uint64(len(line)) + 1
	}

	it.lineOffsets = offsets
	it.reset()
	return nil
}

// ---------------------------------------------------------------------------
// objectLineSource — reads lines from an ObjectReader
// ---------------------------------------------------------------------------

type objectLineSource struct {
	reader ObjectReader
	eof    bool
	binary bool
}

func (s *objectLineSource) readLine() (string, bool, error) {
	if s.eof || s.binary {
		return "", false, nil
	}
	var line []byte
	for {
		var buf [1]byte
		n, err := s.reader.Read(buf[:])
		if n == 0 && err != nil {
			if err == io.EOF {
				s.eof = true
				break
			}
			return "", false, err
		}
		if buf[0] == '\n' {
			break
		}
		if len(line) >= maxLineSize {
			return "", false, errStreamTooLong
		}
		line = append(line, buf[0])
	}
	return string(line), true, nil
}

func (s *objectLineSource) resetSource() {
	s.eof = false
	s.reader.Reset()
}

func (s *objectLineSource) seekToLine(lineNum int, offset uint64) {
	s.eof = false
	s.reader.Reset()
	s.reader.SkipBytes(offset)
}

func (s *objectLineSource) closeSource() {
	if s.reader != nil {
		s.reader.Close()
	}
}

func (s *objectLineSource) isBinary() bool  { return s.binary }
func (s *objectLineSource) setBinary()      { s.binary = true }

// ---------------------------------------------------------------------------
// bufferLineSource — reads lines from an in-memory slice
// ---------------------------------------------------------------------------

type bufferLineSource struct {
	lines       []string
	currentLine int
}

func (s *bufferLineSource) readLine() (string, bool, error) {
	if s.currentLine >= len(s.lines) {
		return "", false, nil
	}
	line := s.lines[s.currentLine]
	s.currentLine++
	return line, true, nil
}

func (s *bufferLineSource) resetSource()                       { s.currentLine = 0 }
func (s *bufferLineSource) seekToLine(lineNum int, _ uint64)   { s.currentLine = lineNum }
func (s *bufferLineSource) closeSource()           {}
func (s *bufferLineSource) isBinary() bool         { return false }
func (s *bufferLineSource) setBinary()             {}

// override seekTo for buffer: just set currentLine (handled by LineIterator.seekTo)
// override next for buffer: use currentLine from the bufferLineSource

// ---------------------------------------------------------------------------
// nothingLineSource
// ---------------------------------------------------------------------------

type nothingLineSource struct{}

func (s *nothingLineSource) readLine() (string, bool, error) { return "", false, nil }
func (s *nothingLineSource) resetSource()                    {}
func (s *nothingLineSource) seekToLine(lineNum int, offset uint64) {}
func (s *nothingLineSource) closeSource()                    {}
func (s *nothingLineSource) isBinary() bool                  { return false }
func (s *nothingLineSource) setBinary()                      {}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

func newLineIteratorFromObject(reader ObjectReader) (*LineIterator, error) {
	it := &LineIterator{
		source: &objectLineSource{reader: reader},
	}
	if err := it.validateLines(); err != nil {
		reader.Close()
		return nil, err
	}
	return it, nil
}

func newLineIteratorFromBuffer(text string) *LineIterator {
	var lines []string
	if text != "" {
		lines = strings.Split(text, "\n")
	}
	offsets := make([]uint64, len(lines))
	var pos uint64
	for i, line := range lines {
		offsets[i] = pos
		pos += uint64(len(line)) + 1
	}
	src := &bufferLineSource{lines: lines}
	return &LineIterator{
		source:      src,
		lineOffsets: offsets,
	}
}

func newLineIteratorFromNothing() *LineIterator {
	return &LineIterator{
		source: &nothingLineSource{},
	}
}

// ---------------------------------------------------------------------------
// Edit
// ---------------------------------------------------------------------------

type editKind int

const (
	editEql editKind = iota
	editIns
	editDel
)

type Edit struct {
	kind       editKind
	oldLineNum int
	newLineNum int
}

// ---------------------------------------------------------------------------
// MyersDiffIterator
// ---------------------------------------------------------------------------

type myersDiffAction int

const (
	myersDiffPush myersDiffAction = iota
	myersDiffPop
)

type myersDiffRange struct {
	delStart int
	delEnd   int
	insStart int
	insEnd   int
}

type MyersDiffIterator struct {
	lineIterA *LineIterator
	lineIterB *LineIterator
	cache     []Edit
	stack     []int
	action    *myersDiffAction
	b         []int
	i, j      int
	n, m      int
	z         int
	rangeMaybe    *myersDiffRange
	deferredRange myersDiffRange
	nextIndex int
	xIndex    int
	yIndex    int
}

func newMyersDiffIterator(lineIterA, lineIterB *LineIterator) *MyersDiffIterator {
	n := lineIterA.count()
	m := lineIterB.count()
	z := (min(n, m) + 1) * 2

	b := make([]int, 2*z)
	action := myersDiffPush

	return &MyersDiffIterator{
		lineIterA: lineIterA,
		lineIterB: lineIterB,
		b:         b,
		action:    &action,
		i:         0,
		j:         0,
		n:         n,
		m:         m,
		z:         z,
	}
}

func (d *MyersDiffIterator) eq(i, j int) (bool, error) {
	lineA, err := d.lineIterA.get(i)
	if err != nil {
		return false, err
	}
	lineB, err := d.lineIterB.get(j)
	if err != nil {
		return false, err
	}
	return lineA == lineB, nil
}

func floorDiv(a, b int) int {
	q := a / b
	if (a^b) < 0 && q*b != a {
		q--
	}
	return q
}

func (d *MyersDiffIterator) diff(action myersDiffAction) (*myersDiffAction, error) {
	i := d.i
	j := d.j
	n := d.n
	m := d.m
	z := d.z

	curAction := action
	for {
		switch curAction {
		case myersDiffPush:
			for n > 0 && m > 0 {
				// clear b
				for k := range d.b {
					d.b[k] = 0
				}

				w := n - m
				l := n + m
				parity := l & 1
				offsetx := i + n - 1
				offsety := j + m - 1
				hmax := (l + parity) / 2

				breakHLoop := false
				for h := 0; h <= hmax; h++ {
					kmin := 2*max(0, h-m) - h
					kmax := h - 2*max(0, h-n)

					// forwards
					for k := kmin; k <= kmax; k += 2 {
						gkm := d.b[k-1-z*floorDiv(k-1, z)]
						gkp := d.b[k+1-z*floorDiv(k+1, z)]
						var u int
						if k == -h || (k != h && gkm < gkp) {
							u = gkp
						} else {
							u = gkm + 1
						}
						v := u - k
						x := u
						y := v
						for x < n && y < m {
							match, err := d.eq(i+x, j+y)
							if err != nil {
								return nil, err
							}
							if !match {
								break
							}
							x++
							y++
						}
						d.b[k-z*floorDiv(k, z)] = x
						if parity == 1 {
							zz := w - k
							if zz >= 1-h && zz < h && x+d.b[z+zz-z*floorDiv(zz, z)] >= n {
								if h > 1 || x != u {
									d.stack = append(d.stack, i+x, n-x, j+y, m-y)
									n = u
									m = v
									z = 2 * (min(n, m) + 1)
									goto continueZBlock
								} else {
									breakHLoop = true
									break
								}
							}
						}
					}
					if breakHLoop {
						break
					}

					// backwards
					for k := kmin; k <= kmax; k += 2 {
						pkm := d.b[z+k-1-z*floorDiv(k-1, z)]
						pkp := d.b[z+k+1-z*floorDiv(k+1, z)]
						var u int
						if k == -h || (k != h && pkm < pkp) {
							u = pkp
						} else {
							u = pkm + 1
						}
						v := u - k
						x := u
						y := v
						for x < n && y < m {
							match, err := d.eq(offsetx-x, offsety-y)
							if err != nil {
								return nil, err
							}
							if !match {
								break
							}
							x++
							y++
						}
						d.b[z+k-z*floorDiv(k, z)] = x
						if parity == 0 {
							zz := w - k
							if zz >= -h && zz <= h && x+d.b[zz-z*floorDiv(zz, z)] >= n {
								if h > 0 || x != u {
									d.stack = append(d.stack, i+n-u, u, j+m-v, v)
									n = n - x
									m = m - y
									z = 2 * (min(n, m) + 1)
									goto continueZBlock
								} else {
									breakHLoop = true
									break
								}
							}
						}
					}
					if breakHLoop {
						break
					}
				}

				if n == m {
					goto continueZBlock
				}
				if m > n {
					i += n
					j += n
					m -= n
					n = 0
				} else {
					i += m
					j += m
					n -= m
					m = 0
				}
				break
			continueZBlock:
			}

			if n+m != 0 {
				if d.rangeMaybe != nil {
					if d.rangeMaybe.delEnd == i || d.rangeMaybe.insEnd == j {
						d.rangeMaybe.delEnd = i + n
						d.rangeMaybe.insEnd = j + m
						curAction = myersDiffPop
						continue
					}
				}

				savedRange := d.rangeMaybe
				d.rangeMaybe = &myersDiffRange{
					delStart: i,
					delEnd:   i + n,
					insStart: j,
					insEnd:   j + m,
				}

				if savedRange != nil {
					d.deferredRange = *savedRange
					d.i = i
					d.n = n
					d.j = j
					d.m = m
					d.z = z
					result := myersDiffPop
					return &result, nil
				}
			}

			curAction = myersDiffPop
			continue

		case myersDiffPop:
			if len(d.stack) == 0 {
				return nil, nil
			}
			sLen := len(d.stack)
			m = d.stack[sLen-1]
			j = d.stack[sLen-2]
			n = d.stack[sLen-3]
			i = d.stack[sLen-4]
			d.stack = d.stack[:sLen-4]
			z = 2 * (min(n, m) + 1)
			curAction = myersDiffPush
			continue
		}
	}
}

func (d *MyersDiffIterator) next() (*Edit, error) {
	if d.nextIndex >= len(d.cache) {
		if d.action == nil {
			return nil, nil
		}
		actionResult, err := d.diff(*d.action)
		if err != nil {
			return nil, err
		}
		d.action = actionResult

		var r myersDiffRange
		if actionResult != nil && *actionResult == myersDiffPop {
			r = d.deferredRange
		} else if d.rangeMaybe != nil {
			r = *d.rangeMaybe
		} else {
			return nil, nil
		}

		// equal lines before the range
		for oi, ni := d.xIndex, d.yIndex; oi < r.delStart && ni < r.insStart; oi, ni = oi+1, ni+1 {
			d.cache = append(d.cache, Edit{kind: editEql, oldLineNum: oi, newLineNum: ni})
		}
		// deletions
		for oi := r.delStart; oi < r.delEnd; oi++ {
			d.cache = append(d.cache, Edit{kind: editDel, oldLineNum: oi})
		}
		// insertions
		for ni := r.insStart; ni < r.insEnd; ni++ {
			d.cache = append(d.cache, Edit{kind: editIns, newLineNum: ni})
		}

		if actionResult == nil {
			// trailing equal lines
			for oi, ni := r.delEnd, r.insEnd; oi < d.lineIterA.count() && ni < d.lineIterB.count(); oi, ni = oi+1, ni+1 {
				d.cache = append(d.cache, Edit{kind: editEql, oldLineNum: oi, newLineNum: ni})
			}
		}

		d.xIndex = r.delEnd
		d.yIndex = r.insEnd
	}

	if d.nextIndex >= len(d.cache) {
		return nil, nil
	}
	edit := d.cache[d.nextIndex]
	d.nextIndex++
	return &edit, nil
}

func (d *MyersDiffIterator) get(oldLine int) (*int, error) {
	// consume all edits
	for {
		e, err := d.next()
		if err != nil {
			return nil, err
		}
		if e == nil {
			break
		}
	}
	for _, edit := range d.cache {
		if edit.kind == editEql {
			if edit.oldLineNum < oldLine {
				continue
			} else if edit.oldLineNum == oldLine {
				n := edit.newLineNum
				return &n, nil
			} else {
				break
			}
		}
	}
	return nil, nil
}

func (d *MyersDiffIterator) contains(oldLine int) (bool, error) {
	result, err := d.get(oldLine)
	if err != nil {
		return false, err
	}
	return result != nil, nil
}

func (d *MyersDiffIterator) reset() {
	d.lineIterA.reset()
	d.lineIterB.reset()
	d.nextIndex = 0
}

// ---------------------------------------------------------------------------
// Diff3Iterator
// ---------------------------------------------------------------------------

type Diff3Range struct {
	Begin int
	End   int
}

type Diff3ChunkKind int

const (
	Diff3Clean    Diff3ChunkKind = iota
	Diff3Conflict
)

type Diff3Chunk struct {
	Kind   Diff3ChunkKind
	ORange *Diff3Range
	ARange *Diff3Range
	BRange *Diff3Range
}

type Diff3Iterator struct {
	lineCountO int
	lineCountA int
	lineCountB int
	lineO      int
	lineA      int
	lineB      int
	myersDiffIterA *MyersDiffIterator
	myersDiffIterB *MyersDiffIterator
	Finished   bool
}

func newDiff3Iterator(lineIterO, lineIterA, lineIterB *LineIterator) *Diff3Iterator {
	myersDiffIterA := newMyersDiffIterator(lineIterO, lineIterA)
	myersDiffIterB := newMyersDiffIterator(lineIterO, lineIterB)
	return &Diff3Iterator{
		lineCountO:     lineIterO.count(),
		lineCountA:     lineIterA.count(),
		lineCountB:     lineIterB.count(),
		myersDiffIterA: myersDiffIterA,
		myersDiffIterB: myersDiffIterB,
	}
}

func (d *Diff3Iterator) next() (*Diff3Chunk, error) {
	if d.Finished {
		return nil, nil
	}

	// find next mismatch
	i := 0
	for d.inBounds(i) {
		matchA, err := d.isMatch(d.myersDiffIterA, d.lineA, i)
		if err != nil {
			return nil, err
		}
		matchB, err := d.isMatch(d.myersDiffIterB, d.lineB, i)
		if err != nil {
			return nil, err
		}
		if !matchA || !matchB {
			break
		}
		i++
	}

	if d.inBounds(i) {
		if i == 0 {
			// find next match
			o := d.lineO
			for o < d.lineCountO {
				containsA, err := d.myersDiffIterA.contains(o)
				if err != nil {
					return nil, err
				}
				containsB, err := d.myersDiffIterB.contains(o)
				if err != nil {
					return nil, err
				}
				if containsA && containsB {
					break
				}
				o++
			}

			a, err := d.myersDiffIterA.get(o)
			if err != nil {
				return nil, err
			}
			if a != nil {
				b, err := d.myersDiffIterB.get(o)
				if err != nil {
					return nil, err
				}
				if b != nil {
					lineO := d.lineO
					lineA := d.lineA
					lineB := d.lineB
					d.lineO = o
					d.lineA = *a
					d.lineB = *b
					return diff3Chunk(
						diff3Range(lineO, d.lineO),
						diff3Range(lineA, d.lineA),
						diff3Range(lineB, d.lineB),
						false,
					), nil
				}
			}
		} else {
			lineO := d.lineO
			lineA := d.lineA
			lineB := d.lineB
			d.lineO += i
			d.lineA += i
			d.lineB += i
			return diff3Chunk(
				diff3Range(lineO, d.lineO),
				diff3Range(lineA, d.lineA),
				diff3Range(lineB, d.lineB),
				true,
			), nil
		}
	}

	// return final chunk
	d.Finished = true
	return diff3Chunk(
		diff3Range(d.lineO, d.lineCountO),
		diff3Range(d.lineA, d.lineCountA),
		diff3Range(d.lineB, d.lineCountB),
		i > 0,
	), nil
}

func (d *Diff3Iterator) reset() {
	d.lineO = 0
	d.lineA = 0
	d.lineB = 0
	d.myersDiffIterA.reset()
	d.myersDiffIterB.reset()
	d.Finished = false
}

func (d *Diff3Iterator) inBounds(i int) bool {
	return d.lineO+i < d.lineCountO ||
		d.lineA+i < d.lineCountA ||
		d.lineB+i < d.lineCountB
}

func (d *Diff3Iterator) isMatch(myersDiffIter *MyersDiffIterator, offset, i int) (bool, error) {
	result, err := myersDiffIter.get(d.lineO + i)
	if err != nil {
		return false, err
	}
	if result != nil {
		return *result == offset+i, nil
	}
	return false, nil
}

func diff3Range(begin, end int) *Diff3Range {
	if end > begin {
		return &Diff3Range{Begin: begin, End: end}
	}
	return nil
}

func diff3Chunk(oRange, aRange, bRange *Diff3Range, match bool) *Diff3Chunk {
	if match {
		if oRange == nil {
			return nil
		}
		return &Diff3Chunk{Kind: Diff3Clean, ORange: oRange}
	}
	return &Diff3Chunk{
		Kind:   Diff3Conflict,
		ORange: oRange,
		ARange: aRange,
		BRange: bRange,
	}
}
