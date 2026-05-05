package engine

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- vocabulary (1000+ unique words) ----

var benchVocab = func() []string {
	raw := `
ability absent accept access account achieve across active actual address
adjust advance advice affect afford afraid agency agree ahead albeit alert
alike align allow almost alone along alter always amend ample angel angry
annual answer appear apply argue arise around arrive aspect assert asset
assign assist assume attach audit avoid aware basic batch beach begin belief
below better beyond black blade blame blank blaze blend bless block bloom
board boost bound brave bread break brief bright bring broad broke brown
build built burst buyer cabin cargo cause chain chair chaos charm chase
cheap check chief child claim clean clear click climb clock close cloud
coach coast color count craft crash crazy cream crime cross crowd curve
cycle dance death debug dense depot depth direct dollar draft drain drama
drawn dream drive early earth eight elite empty enter equal error event
exact exist extra faith false fancy fatal feast fence fetch fiber field
final flame flash fleet float flood floor focus force found frame fresh
front fruit fully ghost giant given glass glide globe grace grade grain
graph grasp grave great green greet grief gross group grove guard guide
guild happy hardy haven health heart heavy hence honor horse hotel human
hurry ideal image index inner input issue jewel joint judge juice knife
labor laser later laugh layer learn least leave legal level light limit
liner local logic loose lower lucky lunar major maker manor maple march
merge metal minor model money moral motor mount mouse movie music naive
nerve never night noble noise north noted novel nurse occur ocean offer
often olive onion onset orbit order outer owner paint panel paper peace
pearl place plain plane plant plate plaza point polar power press price
pride prime print prior prize probe proof prose proud prove proxy pulse
queen query queue quote radar radio range rapid react ready realm relay
reply reset rider right rigid river robot rocky rough round route rural
scale score scout scene scope shade shake shape share sharp shift shore
short sight since skill slack slate sleep slope smart smile smoke solid
solve sonic sound south space spark speed spend spoke sport spray squad
stack staff stage stand start state steel steep stick still stone storm
story study style super surge swift sword system table taste teach terms
theme thick think third throw tiger tight timer tired title today token
topic total touch tough trace track trade trail train trait trend trial
tribe trick trust truth twist ultra under union unity until upper urban
usage usual valid value vault verse video vigor viral visit vital voice
voter waste water weave wedge whole wider world worry worth write yacht
yield young zebra algorithm analysis antenna archive assembly artifact
boolean buffer circuit cluster compute console crypto daemon database
decode deploy device dynamic encode execute fabric filter firmware format
function gateway hardware hashing header hybrid instance kernel lambda
latency library linker loader memory module monitor network neural object
offset packet parallel parser payload pointer process protocol router
runtime scalar schema script server session signal socket software stream
struct template tensor terminal thread timeout transform trigger vector
version virtual webhook alpine anchor arctic aurora autumn azure bamboo
bayou birch bison blizzard boulder brook canyon cedar cliff coral crater
creek delta desert dune ember falcon fern fjord forest fossil frost galaxy
geyser glacier grove harbor hawk horizon hurricane island jungle kelp
lagoon lotus marsh meadow meteor mineral mist monsoon mountain nebula
opal pebble plateau prairie prism quartz rainbow reef ridge sandstone
savanna sediment sequoia sierra silver snowflake solar solstice summit
sunrise terrain tidal timber tornado tropics tundra valley vapor volcano
waterfall wilderness willow admiral advocate ancestor artist athlete
author baron captain citizen council counsel culture delegate doctor
dynasty elder envoy expert faculty farmer founder genius governor herald
historian hunter inventor justice keeper knight lawyer leader legend
merchant mentor minister monarch musician officer oracle patron philosopher
pioneer poet prince professor ranger rebel regent scholar scribe sentinel
settler soldier sovereign speaker steward student surgeon trader veteran
warrior witness wizard abbey attic avenue bakery balcony barrel basket
blanket bottle boutique bridge candle castle cellar chapel chimney closet
cottage counter cradle crate dagger drawer fountain furnace gallery garage
garden gazebo harvest hearth helmet hospice kitchen ladder lantern laundry
locker lodge market mirror mosaic palace pantry parlor passage pedestal
pillar pitcher portal puzzle ranch ribbon saddle shelter shrine stable
staircase statue storage studio tavern temple throne tunnel umbrella
vessel village wardrobe windmill agile ancient austere brilliant brisk
careful cheerful crisp cunning daring delicate elegant enormous fierce
fluid fragile frugal gentle graceful grim harsh hollow humble immense
jolly lofty loyal lucid majestic modest mighty nimble obscure placid
polished precise radiant rigid rugged sacred serene silent sleek slender
solemn sparse steady stoic subtle tender timid tranquil turbulent urgent
valiant vibrant vivid weary zealous achieve adapt advance analyze assert
balance capture challenge combine compile construct convert cultivate
deliver design detect develop discover embrace enforce enhance evolve
explore generate identify implement improve integrate iterate launch
maintain manage measure migrate navigate optimize organize perform publish
reduce refine resolve restore retrieve simulate sustain synthesize validate
verify visualize abyss allegiance allure anthem apex arena ascend astral
atrium ballad bravery bronze cavern cipher citadel clarity cobalt comet
compass conquest cosmos crest crusade crystal descent dignity discord
domain emblem empire epoch essence eternal fable faction fate flaw foray
fury glory harbinger illusion immortal impulse inferno ingenuity insight
irony karma labyrinth legacy lore luster mandate mantle marvel mastery
matrix momentum mosaic motive myth nexus nimbus oath omen origin paradox
paragon passion phantom pinnacle pivot plague plunder potion prophecy
prowess quest relic remedy renown resonance reveal ritual rival rune
rupture saga sanctum scheme scourge scroll seraph shadow siege sigil
silence solace sorcery sorrow specter splendor strife tribute triumph
turmoil twilight upheaval valor vestige vortex voyage warden wraith
zephyr zodiac acumen agony asylum aura avalanche avatar axiom blight
bounty breach burden calamity catalyst champion chronicle clash cloak
command comrade conquer covenant curse darkness decree defiance deity
deluge demon desolate destiny divine dominion dread dusk eclipse elixir
enigma entropy errant exile famine fangs fallen feudal flare flesh flood
forlorn forsaken fortress fracture fragment freedom fringe frontier
gladiator glyph goblin grimoire guardian genre spirit cosmos portal
labyrinth rune sigil shrine sanctum vault realm relics totem scroll
cipher legend heresy prophet decree oracle tribunal citadel monarch
empower endow epoch fertile harvest pinnacle zenith azure cobalt silver
`
	words := strings.Fields(raw)
	seen := make(map[string]bool, len(words))
	unique := make([]string, 0, len(words))
	for _, w := range words {
		if !seen[w] {
			seen[w] = true
			unique = append(unique, w)
		}
	}
	return unique
}()

// ---- helpers ----

func misspellWord(r *rand.Rand, word string) string {
	runes := []rune(word)
	n := len(runes)
	if n < 3 {
		return word
	}
	switch r.Intn(3) {
	case 0: // swap two adjacent chars
		i := r.Intn(n - 1)
		runes[i], runes[i+1] = runes[i+1], runes[i]
	case 1: // delete one char
		i := r.Intn(n)
		runes = append(runes[:i], runes[i+1:]...)
	case 2: // double one char (insert duplicate)
		i := r.Intn(n)
		runes = append(runes[:i+1], append([]rune{runes[i]}, runes[i+1:]...)...)
	}
	return string(runes)
}

func prefixOfWord(r *rand.Rand, word string) string {
	if len(word) <= 4 {
		return word
	}
	// keep 3 to len-2 characters so it's a genuine prefix, not the full word
	minLen := 3
	maxLen := len(word) - 1
	if maxLen <= minLen {
		return word[:minLen]
	}
	return word[:minLen+r.Intn(maxLen-minLen)]
}

// buildSingleQueries returns count single-term query strings.
// 10% misspellings, 25% prefix, rest exact.
func buildSingleQueries(r *rand.Rand, vocab []string, count int) []string {
	qs := make([]string, count)
	for i := range qs {
		word := vocab[r.Intn(len(vocab))]
		roll := r.Float64()
		switch {
		case roll < 0.10:
			word = misspellWord(r, word)
		case roll < 0.35:
			word = prefixOfWord(r, word)
		}
		qs[i] = word
	}
	return qs
}

// buildMultiQueries returns count multi-term query strings (2–4 terms).
// 10% of queries have one misspelled word; last word is prefix 25% of the time.
func buildMultiQueries(r *rand.Rand, vocab []string, count int) []string {
	qs := make([]string, count)
	for i := range qs {
		nTerms := 2 + r.Intn(3) // 2, 3, or 4
		terms := make([]string, nTerms)
		misspellIdx := -1
		if r.Float64() < 0.10 {
			misspellIdx = r.Intn(nTerms)
		}
		for j := 0; j < nTerms; j++ {
			w := vocab[r.Intn(len(vocab))]
			if j == misspellIdx {
				w = misspellWord(r, w)
			} else if j == nTerms-1 && r.Float64() < 0.25 {
				w = prefixOfWord(r, w)
			}
			terms[j] = w
		}
		qs[i] = strings.Join(terms, " ")
	}
	return qs
}

// buildYearFilters returns count year filter maps cycling through 2000–2024.
func buildYearFilters(r *rand.Rand, count int) []map[string][]interface{} {
	fs := make([]map[string][]interface{}, count)
	for i := range fs {
		year := 2000 + r.Intn(25)
		fs[i] = map[string][]interface{}{"year": {year}}
	}
	return fs
}

// ---- corpus ----

const queryPoolSize = 1000

type benchCorpus struct {
	engine      *SearchEngine
	heapMB      float64
	singleExact []string
	multiExact  []string
	yearFilters []map[string][]interface{}
}

var (
	corpus1M benchCorpus
	once1M   sync.Once
	corpus5M benchCorpus
	once5M   sync.Once
)

func getCorpus(n int) *benchCorpus {
	switch n {
	case 1_000_000:
		once1M.Do(func() { corpus1M = buildRealisticCorpus(1_000_000) })
		return &corpus1M
	case 5_000_000:
		once5M.Do(func() { corpus5M = buildRealisticCorpus(5_000_000) })
		return &corpus5M
	}
	panic(fmt.Sprintf("unknown corpus size: %d", n))
}

func buildRealisticCorpus(n int) benchCorpus {
	r := rand.New(rand.NewSource(42))

	se := NewSearchEngine(
		[]string{"title", "tags"},
		map[string]bool{"year": true},
		100,
	)

	docs := make([]map[string]interface{}, n)
	for i := 0; i < n; i++ {
		nTitle := 3 + r.Intn(18) // 3–20 words
		titleParts := make([]string, nTitle)
		for j := range titleParts {
			titleParts[j] = benchVocab[r.Intn(len(benchVocab))]
		}

		nTags := 1 + r.Intn(10) // 1–10 words
		tagParts := make([]string, nTags)
		for j := range tagParts {
			tagParts[j] = benchVocab[r.Intn(len(benchVocab))]
		}

		docs[i] = map[string]interface{}{
			"id":    fmt.Sprintf("d%d", i),
			"title": strings.Join(titleParts, " "),
			"tags":  strings.Join(tagParts, " "),
			"year":  2000 + r.Intn(25),
		}
	}

	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)

	se.Index(docs)

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	heapMB := float64(m1.HeapInuse-m0.HeapInuse) / (1024 * 1024)

	qr := rand.New(rand.NewSource(99))
	return benchCorpus{
		engine:      se,
		heapMB:      heapMB,
		singleExact: buildSingleQueries(qr, benchVocab, queryPoolSize),
		multiExact:  buildMultiQueries(qr, benchVocab, queryPoolSize),
		yearFilters: buildYearFilters(qr, queryPoolSize),
	}
}

// ---- latency helpers ----

func pct(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// runAndMeasure runs b.N iterations of fn, reports p50/p99 via ReportMetric,
// and returns so testing.B can report ns/op, B/op, allocs/op normally.
func runAndMeasure(b *testing.B, fn func()) {
	b.Helper()
	lats := make([]int64, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := time.Now()
		fn()
		lats = append(lats, time.Since(t).Nanoseconds())
	}
	b.StopTimer()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	b.ReportMetric(float64(pct(lats, 0.50)), "p50_ns")
	b.ReportMetric(float64(pct(lats, 0.99)), "p99_ns")
}

// ---- benchmarks ----

func BenchmarkSearch1M(b *testing.B) { runSearchBench(b, 1_000_000) }
func BenchmarkSearch5M(b *testing.B) { runSearchBench(b, 5_000_000) }

func runSearchBench(b *testing.B, n int) {
	b.Helper()
	c := getCorpus(n)
	label := fmt.Sprintf("%dM_docs", n/1_000_000)

	b.Run(fmt.Sprintf("SingleTerm/NoFilter/%s", label), func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(c.heapMB, "heap_MB")
		idx := 0
		runAndMeasure(b, func() {
			_ = c.engine.Search(c.singleExact[idx%queryPoolSize], nil)
			idx++
		})
	})

	b.Run(fmt.Sprintf("SingleTerm/Filter/%s", label), func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(c.heapMB, "heap_MB")
		idx := 0
		runAndMeasure(b, func() {
			_ = c.engine.Search(c.singleExact[idx%queryPoolSize], c.yearFilters[idx%queryPoolSize])
			idx++
		})
	})

	b.Run(fmt.Sprintf("MultiTerm/NoFilter/%s", label), func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(c.heapMB, "heap_MB")
		idx := 0
		runAndMeasure(b, func() {
			_ = c.engine.Search(c.multiExact[idx%queryPoolSize], nil)
			idx++
		})
	})

	b.Run(fmt.Sprintf("MultiTerm/Filter/%s", label), func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(c.heapMB, "heap_MB")
		idx := 0
		runAndMeasure(b, func() {
			_ = c.engine.Search(c.multiExact[idx%queryPoolSize], c.yearFilters[idx%queryPoolSize])
			idx++
		})
	})
}
