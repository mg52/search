// loadtest hammers the search endpoint with realistic queries drawn from the
// same vocabulary used by the engine benchmarks. It reports aggregate latency
// (avg/p50/p95/p99), RPS, and a per-mode breakdown (single/multi × no-filter/filter).
//
// Usage:
//
//	go run ./loadtest -url http://localhost:8080/search -index bench -requests 10000
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---- vocabulary (same as datagen / bench_test.go) ----

var vocab = func() []string {
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

// ---- query helpers ----

func misspellWord(r *rand.Rand, word string) string {
	runes := []rune(word)
	n := len(runes)
	if n < 3 {
		return word
	}
	switch r.Intn(3) {
	case 0:
		i := r.Intn(n - 1)
		runes[i], runes[i+1] = runes[i+1], runes[i]
	case 1:
		i := r.Intn(n)
		runes = append(runes[:i], runes[i+1:]...)
	case 2:
		i := r.Intn(n)
		runes = append(runes[:i+1], append([]rune{runes[i]}, runes[i+1:]...)...)
	}
	return string(runes)
}

func prefixOfWord(r *rand.Rand, word string) string {
	if len(word) <= 4 {
		return word
	}
	minLen := 3
	maxLen := len(word) - 1
	if maxLen <= minLen {
		return word[:minLen]
	}
	return word[:minLen+r.Intn(maxLen-minLen)]
}

// singleTermQuery returns one query token: 10% misspell, 25% prefix, 65% exact.
func singleTermQuery(r *rand.Rand) string {
	word := vocab[r.Intn(len(vocab))]
	roll := r.Float64()
	switch {
	case roll < 0.10:
		return misspellWord(r, word)
	case roll < 0.35:
		return prefixOfWord(r, word)
	default:
		return word
	}
}

// multiTermQuery returns 2-4 tokens joined by space.
// 10% chance one token is misspelled; last token is a prefix 25% of the time.
func multiTermQuery(r *rand.Rand) string {
	nTerms := 2 + r.Intn(3)
	terms := make([]string, nTerms)
	misspellIdx := -1
	if r.Float64() < 0.10 {
		misspellIdx = r.Intn(nTerms)
	}
	for j := 0; j < nTerms; j++ {
		w := vocab[r.Intn(len(vocab))]
		switch {
		case j == misspellIdx:
			w = misspellWord(r, w)
		case j == nTerms-1 && r.Float64() < 0.25:
			w = prefixOfWord(r, w)
		}
		terms[j] = w
	}
	return strings.Join(terms, " ")
}

func yearFilter(r *rand.Rand) string {
	return fmt.Sprintf("year:%d", 2000+r.Intn(25))
}

// ---- result tracking ----

type modeKey struct {
	multi  bool
	filter bool
}

func (m modeKey) String() string {
	q := "Single"
	if m.multi {
		q = "Multi "
	}
	f := "NoFilter"
	if m.filter {
		f = "Filter  "
	}
	return q + " / " + f
}

type result struct {
	lat  time.Duration
	mode modeKey
	err  bool
}

// ---- main ----

func main() {
	baseURL := flag.String("url", "http://localhost:8080/search", "Search endpoint URL")
	indexName := flag.String("index", "bench", "Index name (?index=)")
	workers := flag.Int("workers", 8, "Number of parallel workers")
	requests := flag.Int("requests", 10_000, "Total requests to send")
	timeout := flag.Duration("timeout", 10*time.Second, "Per-request HTTP timeout")
	seed := flag.Int64("seed", time.Now().UnixNano(), "Random seed")
	keepAlive := flag.Bool("keepalive", true, "Use HTTP keep-alive")
	filterPct := flag.Int("filter-pct", 50, "Percentage of requests that include a year filter (0-100)")
	multiPct := flag.Int("multi-pct", 50, "Percentage of requests that use multi-term queries (0-100)")
	flag.Parse()

	tr := &http.Transport{
		DisableKeepAlives:   !*keepAlive,
		MaxIdleConns:        1000,
		MaxConnsPerHost:     0,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{Timeout: *timeout, Transport: tr}

	var sent int64
	total := int64(*requests)

	results := make([]result, 0, *requests)
	var resultsMu sync.Mutex
	var errCount int64

	fmt.Printf("Target:   %s  index=%s\n", *baseURL, *indexName)
	fmt.Printf("Workers:  %d  |  Requests: %d  |  filter-pct: %d%%  |  multi-pct: %d%%\n",
		*workers, *requests, *filterPct, *multiPct)
	fmt.Println("Starting...")

	startAll := time.Now()
	var wg sync.WaitGroup
	wg.Add(*workers)

	for w := 0; w < *workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(*seed + int64(workerID)*1_000_003))

			for {
				i := atomic.AddInt64(&sent, 1)
				if i > total {
					return
				}

				isMulti := r.Intn(100) < *multiPct
				isFilter := r.Intn(100) < *filterPct

				var q string
				if isMulti {
					q = multiTermQuery(r)
				} else {
					q = singleTermQuery(r)
				}

				reqURL := buildURL(*baseURL, *indexName, q, isFilter, r)

				t0 := time.Now()
				req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, reqURL, nil)
				resp, err := client.Do(req)
				lat := time.Since(t0)

				res := result{
					lat:  lat,
					mode: modeKey{multi: isMulti, filter: isFilter},
					err:  err != nil,
				}

				if err != nil {
					atomic.AddInt64(&errCount, 1)
				} else {
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						atomic.AddInt64(&errCount, 1)
						res.err = true
					}
					_ = resp.Body.Close()
				}

				resultsMu.Lock()
				results = append(results, res)
				resultsMu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(startAll)

	if len(results) == 0 {
		fmt.Println("No results collected.")
		os.Exit(1)
	}

	// ---- overall stats ----
	allLats := make([]time.Duration, len(results))
	for i, r := range results {
		allLats[i] = r.lat
	}
	sort.Slice(allLats, func(i, j int) bool { return allLats[i] < allLats[j] })

	rps := float64(len(results)) / elapsed.Seconds()

	fmt.Printf("\n──────────────────────────────────────────────────────\n")
	fmt.Printf("Requests: %-6d  Workers: %-3d  Errors: %-4d  GOMAXPROCS: %d\n",
		len(results), *workers, atomic.LoadInt64(&errCount), runtime.GOMAXPROCS(0))
	fmt.Printf("Wall time: %-12s  RPS: %.1f\n", elapsed.Round(time.Millisecond), rps)
	fmt.Printf("──────────────────────────────────────────────────────\n")
	fmt.Println("Overall latency:")
	printLatencyRow("  all", allLats)

	// ---- per-mode breakdown ----
	byMode := make(map[modeKey][]time.Duration)
	for _, r := range results {
		byMode[r.mode] = append(byMode[r.mode], r.lat)
	}

	modes := []modeKey{
		{multi: false, filter: false},
		{multi: false, filter: true},
		{multi: true, filter: false},
		{multi: true, filter: true},
	}

	fmt.Printf("\nPer-mode breakdown:\n")
	fmt.Printf("  %-24s  %8s  %8s  %8s  %8s  %6s\n", "mode", "avg", "p50", "p95", "p99", "count")
	fmt.Printf("  %-24s  %8s  %8s  %8s  %8s  %6s\n", "----", "---", "---", "---", "---", "-----")
	for _, m := range modes {
		lats := byMode[m]
		if len(lats) == 0 {
			continue
		}
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		fmt.Printf("  %-24s  %8s  %8s  %8s  %8s  %6d\n",
			m,
			fmtMs(average(lats)),
			fmtMs(pct(lats, 50)),
			fmtMs(pct(lats, 95)),
			fmtMs(pct(lats, 99)),
			len(lats),
		)
	}
	fmt.Printf("──────────────────────────────────────────────────────\n")
}

func buildURL(base, index, q string, addFilter bool, r *rand.Rand) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	params := u.Query()
	params.Set("index", index)
	params.Set("q", q)
	if addFilter {
		params.Set("filter", yearFilter(r))
	}
	u.RawQuery = params.Encode()
	return u.String()
}

func printLatencyRow(label string, sorted []time.Duration) {
	fmt.Printf("  %-8s  avg=%-10s  p50=%-10s  p95=%-10s  p99=%s\n",
		label,
		fmtMs(average(sorted)),
		fmtMs(pct(sorted, 50)),
		fmtMs(pct(sorted, 95)),
		fmtMs(pct(sorted, 99)),
	)
}

func fmtMs(d time.Duration) string {
	return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
}

func average(lats []time.Duration) time.Duration {
	var sum int64
	for _, d := range lats {
		sum += int64(d)
	}
	return time.Duration(sum / int64(len(lats)))
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	n := len(sorted)
	k := (p*n + 99) / 100
	idx := k - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}
