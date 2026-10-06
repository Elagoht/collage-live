// A collage plugin that keeps parts of a page current in the browser: a small
// client that refreshes fragment paths on an interval, and an event stream that
// pushes a fragment when a tag it depends on is invalidated.
//
// It requires collage the way any consumer does, and reaches nothing the framework
// does not offer every plugin.
module github.com/Elagoht/collage-live

go 1.26

require github.com/Elagoht/collage v0.49.0
