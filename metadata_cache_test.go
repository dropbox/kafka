package kafka

import (
	. "gopkg.in/check.v1"
)

var _ = Suite(&MetadataCacheSuite{})

type MetadataCacheSuite struct{}

func (s *MetadataCacheSuite) SetUpTest(c *C) {
	ResetTestLogger(c)
}

func (s *MetadataCacheSuite) TestBasic(c *C) {
	//cache := NewMetadataCache()
	//addresses := []string{"foo:000", "bar:000", "baz:000"}
	//metadataFromCache, err := cache.getOrCreateMetadata(addresses)
	//sameMetadataFromCache, err := cache.getOrCreateMetadata(addresses)
	//if err != nil {
	//	c.Fail()
	//}
	//c.Assert(metadataFromCache, Equals, sameMetadataFromCache)
}
