// datagen generates a JSON file of synthetic documents using the same
// vocabulary and field structure as the engine benchmarks.
//
// Usage:
//
//	go run ./cmd/datagen -count 1000000 -out data.json
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

// Same vocabulary as internal/engine/bench_test.go
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

func main() {
	count := flag.Int("count", 1_000_000, "Number of documents to generate")
	out := flag.String("out", "data.json", "Output JSON file path")
	seed := flag.Int64("seed", 42, "Random seed")
	flag.Parse()

	r := rand.New(rand.NewSource(*seed))

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", *out, err)
		os.Exit(1)
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 8*1024*1024)

	fmt.Printf("Generating %d documents → %s\n", *count, *out)
	start := time.Now()

	bw.WriteString("[\n")

	enc := json.NewEncoder(bw)
	for i := 0; i < *count; i++ {
		if i > 0 {
			bw.WriteString(",\n")
		}

		nTitle := 3 + r.Intn(18) // 3–20 words
		titleParts := make([]string, nTitle)
		for j := range titleParts {
			titleParts[j] = vocab[r.Intn(len(vocab))]
		}

		nTags := 1 + r.Intn(10) // 1–10 words
		tagParts := make([]string, nTags)
		for j := range tagParts {
			tagParts[j] = vocab[r.Intn(len(vocab))]
		}

		doc := map[string]interface{}{
			"id":    fmt.Sprintf("d%d", i),
			"title": strings.Join(titleParts, " "),
			"tags":  strings.Join(tagParts, " "),
			"year":  2000 + r.Intn(25),
		}

		if err := enc.Encode(doc); err != nil {
			fmt.Fprintf(os.Stderr, "encode doc %d: %v\n", i, err)
			os.Exit(1)
		}

		if (i+1)%100_000 == 0 || i+1 == *count {
			fmt.Printf("  %d / %d\n", i+1, *count)
		}
	}

	bw.WriteString("]\n")
	if err := bw.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
		os.Exit(1)
	}

	fi, _ := f.Stat()
	fmt.Printf("Done in %s — %.1f MB\n", time.Since(start).Round(time.Millisecond), float64(fi.Size())/(1024*1024))
}
