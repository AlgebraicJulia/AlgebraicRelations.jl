using ACSets
using Catlab
using AlgebraicRelations

using SQLite, DBInterface
using FunSQL

τ = AlgebraicRelations.SQL.DatabaseDS.DBSourceTrait()
fabric = DataFabric()

# ═══════════════════════════════════════════════════════════
# Schema: Music Industry
#
#   Artist ←─ ArtistAlbum ─→ Album ←─ AlbumTrack ─→ Track
#     ↑                        ↑                       ↑
#  ArtistGenre              AlbumLabel              TrackGenre
#     ↓                        ↓                       ↓
#   Genre ←──────────────────────────────────────── Genre (shared)
#                            Label
#
#   Venue ←─ VenueArtist ─→ Artist (shared)
#     ↑
#  VenueCity
#     ↓
#   City
#
# Cyclic query: "Find artists who play a genre AND have a track
#   tagged with that same genre, at venues in a specific city"
#   Cycle: artist → genre ← track → album ← artist
# ═══════════════════════════════════════════════════════════

# ─── Genre (InMemory) ───
@present SchGenre(FreeSchema) begin
    Name::AttrType
    Genre::Ob
    genre_name::Attr(Genre, Name)
end
@acset_type GenreT(SchGenre)
genre = InMemory(GenreT{Symbol}())
genre_src = add_source!(fabric, genre)

add_part!(fabric, :Genre, genre_name=:Rock)        # 1
add_part!(fabric, :Genre, genre_name=:Jazz)         # 2
add_part!(fabric, :Genre, genre_name=:Electronic)   # 3
add_part!(fabric, :Genre, genre_name=:Classical)     # 4
add_part!(fabric, :Genre, genre_name=:HipHop)        # 5
add_part!(fabric, :Genre, genre_name=:Blues)          # 6

# ─── Artist (InMemory) ───
@present SchArtist(FreeSchema) begin
    Name::AttrType
    Artist::Ob
    artist_name::Attr(Artist, Name)
end
@acset_type ArtistT(SchArtist)
artist = InMemory(ArtistT{Symbol}())
artist_src = add_source!(fabric, artist)

add_part!(fabric, :Artist, artist_name=:Radiohead)      # 1
add_part!(fabric, :Artist, artist_name=:MilesDavis)      # 2
add_part!(fabric, :Artist, artist_name=:Bjork)           # 3
add_part!(fabric, :Artist, artist_name=:YoYoMa)          # 4
add_part!(fabric, :Artist, artist_name=:KendrickLamar)   # 5
add_part!(fabric, :Artist, artist_name=:BBKing)           # 6
add_part!(fabric, :Artist, artist_name=:Portishead)      # 7
add_part!(fabric, :Artist, artist_name=:NinaSimone)      # 8

# ─── ArtistGenre (InMemory) ── artist plays genre ───
@present SchArtistGenre(FreeSchema) begin
    (Artist, Genre)::AttrType
    ArtistGenre::Ob
    ag_artist::Attr(ArtistGenre, Artist)
    ag_genre::Attr(ArtistGenre, Genre)
end
@acset_type ArtistGenreT(SchArtistGenre)
artist_genre = InMemory(ArtistGenreT{FK{ArtistT}, FK{GenreT}}())
artist_genre_src = add_source!(fabric, artist_genre)
add_fk!(fabric, artist_genre_src, artist_src, :ArtistGenre!ag_artist => :Artist!Artist_id)
add_fk!(fabric, artist_genre_src, genre_src, :ArtistGenre!ag_genre => :Genre!Genre_id)

add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(1), ag_genre=FK{GenreT}(1))  # Radiohead: Rock
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(1), ag_genre=FK{GenreT}(3))  # Radiohead: Electronic
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(2), ag_genre=FK{GenreT}(2))  # MilesDavis: Jazz
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(2), ag_genre=FK{GenreT}(6))  # MilesDavis: Blues
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(3), ag_genre=FK{GenreT}(3))  # Bjork: Electronic
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(3), ag_genre=FK{GenreT}(1))  # Bjork: Rock
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(4), ag_genre=FK{GenreT}(4))  # YoYoMa: Classical
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(5), ag_genre=FK{GenreT}(5))  # KendrickLamar: HipHop
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(6), ag_genre=FK{GenreT}(6))  # BBKing: Blues
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(6), ag_genre=FK{GenreT}(1))  # BBKing: Rock
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(7), ag_genre=FK{GenreT}(3))  # Portishead: Electronic
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(7), ag_genre=FK{GenreT}(1))  # Portishead: Rock
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(8), ag_genre=FK{GenreT}(2))  # NinaSimone: Jazz
add_part!(fabric, :ArtistGenre, ag_artist=FK{ArtistT}(8), ag_genre=FK{GenreT}(6))  # NinaSimone: Blues

# ─── Album (InMemory) ───
@present SchAlbum(FreeSchema) begin
    Name::AttrType
    Album::Ob
    album_name::Attr(Album, Name)
    release_year::Attr(Album, Name)
end
@acset_type AlbumT(SchAlbum)
album = InMemory(AlbumT{Symbol}())
album_src = add_source!(fabric, album)

add_part!(fabric, :Album, album_name=:OKComputer,       release_year=Symbol("1997"))  # 1
add_part!(fabric, :Album, album_name=:KidA,              release_year=Symbol("2000"))  # 2
add_part!(fabric, :Album, album_name=:KindOfBlue,        release_year=Symbol("1959"))  # 3
add_part!(fabric, :Album, album_name=:Homogenic,         release_year=Symbol("1997"))  # 4
add_part!(fabric, :Album, album_name=:BachCelloSuites,   release_year=Symbol("1983"))  # 5
add_part!(fabric, :Album, album_name=:DAMN,              release_year=Symbol("2017"))  # 6
add_part!(fabric, :Album, album_name=:LiveAtTheRegal,    release_year=Symbol("1965"))  # 7
add_part!(fabric, :Album, album_name=:Dummy,             release_year=Symbol("1994"))  # 8
add_part!(fabric, :Album, album_name=:Bitches_Brew,      release_year=Symbol("1970"))  # 9
add_part!(fabric, :Album, album_name=:Vespertine,        release_year=Symbol("2001"))  # 10

# ─── ArtistAlbum (InMemory) ───
@present SchArtistAlbum(FreeSchema) begin
    (Artist, Album)::AttrType
    ArtistAlbum::Ob
    aa_artist::Attr(ArtistAlbum, Artist)
    aa_album::Attr(ArtistAlbum, Album)
end
@acset_type ArtistAlbumT(SchArtistAlbum)
artist_album = InMemory(ArtistAlbumT{FK{ArtistT}, FK{AlbumT}}())
artist_album_src = add_source!(fabric, artist_album)
add_fk!(fabric, artist_album_src, artist_src, :ArtistAlbum!aa_artist => :Artist!Artist_id)
add_fk!(fabric, artist_album_src, album_src, :ArtistAlbum!aa_album => :Album!Album_id)

add_part!(fabric, :ArtistAlbum, aa_artist=FK{ArtistT}(1), aa_album=FK{AlbumT}(1))  # Radiohead: OKComputer
add_part!(fabric, :ArtistAlbum, aa_artist=FK{ArtistT}(1), aa_album=FK{AlbumT}(2))  # Radiohead: KidA
add_part!(fabric, :ArtistAlbum, aa_artist=FK{ArtistT}(2), aa_album=FK{AlbumT}(3))  # MilesDavis: KindOfBlue
add_part!(fabric, :ArtistAlbum, aa_artist=FK{ArtistT}(2), aa_album=FK{AlbumT}(9))  # MilesDavis: Bitches_Brew
add_part!(fabric, :ArtistAlbum, aa_artist=FK{ArtistT}(3), aa_album=FK{AlbumT}(4))  # Bjork: Homogenic
add_part!(fabric, :ArtistAlbum, aa_artist=FK{ArtistT}(3), aa_album=FK{AlbumT}(10)) # Bjork: Vespertine
add_part!(fabric, :ArtistAlbum, aa_artist=FK{ArtistT}(4), aa_album=FK{AlbumT}(5))  # YoYoMa: BachCelloSuites
add_part!(fabric, :ArtistAlbum, aa_artist=FK{ArtistT}(5), aa_album=FK{AlbumT}(6))  # KendrickLamar: DAMN
add_part!(fabric, :ArtistAlbum, aa_artist=FK{ArtistT}(6), aa_album=FK{AlbumT}(7))  # BBKing: LiveAtTheRegal
add_part!(fabric, :ArtistAlbum, aa_artist=FK{ArtistT}(7), aa_album=FK{AlbumT}(8))  # Portishead: Dummy

# ─── Track (SQLite) ───
@present SchTrack(FreeSchema) begin
    (Name, Album)::AttrType
    Track::Ob
    track_name::Attr(Track, Name)
    tr_album::Attr(Track, Album)
end
@acset_type TrackT(SchTrack)
track_acset = TrackT{Symbol, FK{AlbumT}}()
track_db = DBSource(SQLite.DB(), track_acset)
execute![τ](track_db, FunSQL.render(track_db, track_acset))
track_src = add_source!(fabric, track_db, :Track) # TODO why :Track
add_fk!(fabric, track_src, album_src, :Track!tr_album => :Album!Album_id)

add_part!(fabric, :Track, [
    (_id=1,  track_name=:ParanoidAndroid,  tr_album=FK{AlbumT}(1)),
    (_id=2,  track_name=:KarmaPolice,      tr_album=FK{AlbumT}(1)),
    (_id=3,  track_name=:EverythingInItsRightPlace, tr_album=FK{AlbumT}(2)),
    (_id=4,  track_name=:Idioteque,        tr_album=FK{AlbumT}(2)),
    (_id=5,  track_name=:SoWhat,           tr_album=FK{AlbumT}(3)),
    (_id=6,  track_name=:BlueInGreen,      tr_album=FK{AlbumT}(3)),
    (_id=7,  track_name=:Joga,             tr_album=FK{AlbumT}(4)),
    (_id=8,  track_name=:SuiteNo1,         tr_album=FK{AlbumT}(5)),
    (_id=9,  track_name=:HUMBLE,           tr_album=FK{AlbumT}(6)),
    (_id=10, track_name=:DNA,              tr_album=FK{AlbumT}(6)),
    (_id=11, track_name=:EverydayIHaveTheBlues, tr_album=FK{AlbumT}(7)),
    (_id=12, track_name=:SweetLittleAngel, tr_album=FK{AlbumT}(7)),
    (_id=13, track_name=:Wandering_Star,   tr_album=FK{AlbumT}(8)),
    (_id=14, track_name=:GloryBox,         tr_album=FK{AlbumT}(8)),
    (_id=15, track_name=:SpanishKey,       tr_album=FK{AlbumT}(9)),
    (_id=16, track_name=:Pharaohs_Dance,   tr_album=FK{AlbumT}(9)),
    (_id=17, track_name=:Pagan_Poetry,     tr_album=FK{AlbumT}(10)),
    (_id=18, track_name=:Hidden_Place,     tr_album=FK{AlbumT}(10)),
])

# ─── TrackGenre (SQLite) ── each track is tagged with a genre ───
@present SchTrackGenre(FreeSchema) begin
    (Track, Genre)::AttrType
    TrackGenre::Ob
    tg_track::Attr(TrackGenre, Track)
    tg_genre::Attr(TrackGenre, Genre)
end
@acset_type TrackGenreT(SchTrackGenre)
track_genre_acset = TrackGenreT{FK{TrackT}, FK{GenreT}}()
track_genre_db = DBSource(SQLite.DB(), track_genre_acset)
execute![τ](track_genre_db, FunSQL.render(track_genre_db, track_genre_acset))
track_genre_src = add_source!(fabric, track_genre_db, :TrackGenre)
add_fk!(fabric, track_genre_src, track_src, :TrackGenre!tg_track => :Track!Track_id)
add_fk!(fabric, track_genre_src, genre_src, :TrackGenre!tg_genre => :Genre!Genre_id)

# XXX tg_genre and ag_genre need to be treated as attributes in the fabric
add_part!(fabric, :TrackGenre, [
    (_id=1,  tg_track=FK{TrackT}(1),  tg_genre=FK{GenreT}(1)),  # ParanoidAndroid: Rock
    (_id=2,  tg_track=FK{TrackT}(2),  tg_genre=FK{GenreT}(1)),  # KarmaPolice: Rock
    (_id=3,  tg_track=FK{TrackT}(3),  tg_genre=FK{GenreT}(3)),  # EverythingInItsRightPlace: Electronic
    (_id=4,  tg_track=FK{TrackT}(4),  tg_genre=FK{GenreT}(3)),  # Idioteque: Electronic
    (_id=5,  tg_track=FK{TrackT}(5),  tg_genre=FK{GenreT}(2)),  # SoWhat: Jazz
    (_id=6,  tg_track=FK{TrackT}(6),  tg_genre=FK{GenreT}(2)),  # BlueInGreen: Jazz
    (_id=7,  tg_track=FK{TrackT}(7),  tg_genre=FK{GenreT}(3)),  # Joga: Electronic
    (_id=8,  tg_track=FK{TrackT}(8),  tg_genre=FK{GenreT}(4)),  # SuiteNo1: Classical
    (_id=9,  tg_track=FK{TrackT}(9),  tg_genre=FK{GenreT}(5)),  # HUMBLE: HipHop
    (_id=10, tg_track=FK{TrackT}(10), tg_genre=FK{GenreT}(5)),  # DNA: HipHop
    (_id=11, tg_track=FK{TrackT}(11), tg_genre=FK{GenreT}(6)),  # EverydayIHaveTheBlues: Blues
    (_id=12, tg_track=FK{TrackT}(12), tg_genre=FK{GenreT}(6)),  # SweetLittleAngel: Blues
    (_id=13, tg_track=FK{TrackT}(13), tg_genre=FK{GenreT}(3)),  # Wandering_Star: Electronic
    (_id=14, tg_track=FK{TrackT}(14), tg_genre=FK{GenreT}(1)),  # GloryBox: Rock
    (_id=15, tg_track=FK{TrackT}(15), tg_genre=FK{GenreT}(2)),  # SpanishKey: Jazz
    (_id=16, tg_track=FK{TrackT}(16), tg_genre=FK{GenreT}(2)),  # Pharaohs_Dance: Jazz
    (_id=17, tg_track=FK{TrackT}(17), tg_genre=FK{GenreT}(3)),  # Pagan_Poetry: Electronic
    (_id=18, tg_track=FK{TrackT}(18), tg_genre=FK{GenreT}(3)),  # Hidden_Place: Electronic
])

# ─── City (InMemory) ───
@present SchCity(FreeSchema) begin
    Name::AttrType
    City::Ob
    city_name::Attr(City, Name)
end
@acset_type CityT(SchCity)
city = InMemory(CityT{Symbol}())
city_src = add_source!(fabric, city)

add_part!(fabric, :City, city_name=:London)      # 1
add_part!(fabric, :City, city_name=:NewYork)     # 2
add_part!(fabric, :City, city_name=:Tokyo)       # 3
add_part!(fabric, :City, city_name=:Berlin)      # 4
add_part!(fabric, :City, city_name=:LosAngeles)  # 5

# ─── Venue (InMemory) ───
@present SchVenue(FreeSchema) begin
    (Name, City)::AttrType
    Venue::Ob
    venue_name::Attr(Venue, Name)
    ve_city::Attr(Venue, City)
end
@acset_type VenueT(SchVenue)
venue = InMemory(VenueT{Symbol, FK{CityT}}())
venue_src = add_source!(fabric, venue)
add_fk!(fabric, venue_src, city_src, :Venue!ve_city => :City!City_id)

add_part!(fabric, :Venue, venue_name=:RoyalAlbertHall,   ve_city=FK{CityT}(1))  # 1 London
add_part!(fabric, :Venue, venue_name=:Barbican,          ve_city=FK{CityT}(1))  # 2 London
add_part!(fabric, :Venue, venue_name=:MadisonSquareGarden,ve_city=FK{CityT}(2)) # 3 NYC
add_part!(fabric, :Venue, venue_name=:BlueNote,          ve_city=FK{CityT}(2))  # 4 NYC
add_part!(fabric, :Venue, venue_name=:Budokan,           ve_city=FK{CityT}(3))  # 5 Tokyo
add_part!(fabric, :Venue, venue_name=:Berghain,          ve_city=FK{CityT}(4))  # 6 Berlin
add_part!(fabric, :Venue, venue_name=:Hollywood_Bowl,    ve_city=FK{CityT}(5))  # 7 LA

# ─── VenueArtist (InMemory) ── artist has played at venue ───
@present SchVenueArtist(FreeSchema) begin
    (Venue, Artist)::AttrType
    VenueArtist::Ob
    va_venue::Attr(VenueArtist, Venue)
    va_artist::Attr(VenueArtist, Artist)
end
@acset_type VenueArtistT(SchVenueArtist)
venue_artist = InMemory(VenueArtistT{FK{VenueT}, FK{ArtistT}}())
venue_artist_src = add_source!(fabric, venue_artist)
add_fk!(fabric, venue_artist_src, venue_src, :VenueArtist!va_venue => :Venue!Venue_id)
add_fk!(fabric, venue_artist_src, artist_src, :VenueArtist!va_artist => :Artist!Artist_id)

add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(1), va_artist=FK{ArtistT}(1))  # Radiohead @ RoyalAlbertHall
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(3), va_artist=FK{ArtistT}(1))  # Radiohead @ MSG
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(4), va_artist=FK{ArtistT}(2))  # MilesDavis @ BlueNote
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(2), va_artist=FK{ArtistT}(3))  # Bjork @ Barbican
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(6), va_artist=FK{ArtistT}(3))  # Bjork @ Berghain
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(2), va_artist=FK{ArtistT}(4))  # YoYoMa @ Barbican
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(3), va_artist=FK{ArtistT}(5))  # KendrickLamar @ MSG
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(7), va_artist=FK{ArtistT}(5))  # KendrickLamar @ Hollywood_Bowl
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(4), va_artist=FK{ArtistT}(6))  # BBKing @ BlueNote
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(1), va_artist=FK{ArtistT}(7))  # Portishead @ RoyalAlbertHall
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(5), va_artist=FK{ArtistT}(7))  # Portishead @ Budokan
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(4), va_artist=FK{ArtistT}(8))  # NinaSimone @ BlueNote
add_part!(fabric, :VenueArtist, va_venue=FK{VenueT}(3), va_artist=FK{ArtistT}(8))  # NinaSimone @ MSG

# ═══════════════════════════════════════════════════════════
# Queries
# ═══════════════════════════════════════════════════════════

# Query 1 (cyclic + filter): "Find artists who play a genre AND have
#   tracks tagged with that same genre, who have played at venues
#   in London. Return the artist name, genre, and track name."
#
# Cycle: artist → genre (via ArtistGenre)
#        artist → album → track → genre (via TrackGenre)
# The genre junction `gn` is shared, closing the cycle.
# Filter: city = London
#
# Expected results include:
#   Radiohead plays Rock, has Rock tracks (ParanoidAndroid, KarmaPolice)
#   Radiohead plays Electronic, has Electronic tracks (EverythingInItsRightPlace, Idioteque)
#   Portishead plays Rock, has Rock track (GloryBox)
#   Portishead plays Electronic, has Electronic tracks (Wandering_Star)
#
q_cyclic = @relation (aname=an, gname=gn_name, tname=tn) begin
    # filter first for early pruning
    CityFilter(city_name=city_name) # TODO need to safely handle when the portname is not given
    City(id=ct, city_name=city_name)  
    Venue(id=v, ve_city=ct) # TODO add a city ref table
    VenueArtist(va_venue=v, va_artist=a)
    # artist-genre link
    ArtistGenre(ag_artist=a, ag_genre=gn)
    # artist-album-track-genre path (closes cycle on gn)
    ArtistAlbum(aa_artist=a, aa_album=al)
    Track(id=t, tr_album=al, track_name=tn)
    TrackGenre(tg_track=t, tg_genre=gn)
    # decode names
    Artist(id=a, artist_name=an)
    Genre(id=gn, genre_name=gn_name)
end

y = prepare(q_cyclic, fabric, filters=Dict(:city_name=>:Berlin))

lookup = y[2]
_df = map(eachcol(y[1])) do col
    # Journal.field, Keyword.keyword, Grant.ag, Department.dept_name
    (Artist=lookup[:Artist][:artist_name][col[1]],
     Genre=lookup[:Genre][:genre_name][col[2]],
     Track=lookup[:Track][:track_name][col[3]])
end

using DataFrames

df=DataFrame(_df)

# To run with London filter (city 1):
# prepare(q_cyclic, fabric, filters=Dict(:CityFilter => [1;;]))

# Query 2 (simple): "What tracks are on each album?"
q_simple = @relation (aname=an, tname=tn) begin
    ArtistAlbum(aa_artist=a, aa_album=al)
    Track(id=t, tr_album=al)
    Artist(id=a, artist_name=an)
    Track(id=t, track_name=tn)
end

# Query 3 (star from track): "For a given track, find its genre,
#   album, artist, and which venues the artist has played"
q_star = @relation (tname=tn, gname=gn, aname=an, vname=vn) begin
    TrackGenre(tg_track=t, tg_genre=g)
    Track(id=t, tr_album=al, track_name=tn)
    ArtistAlbum(aa_album=al, aa_artist=a)
    VenueArtist(va_artist=a, va_venue=v)
    Genre(id=g, genre_name=gn)
    Artist(id=a, artist_name=an)
    Venue(id=v, venue_name=vn)
end


