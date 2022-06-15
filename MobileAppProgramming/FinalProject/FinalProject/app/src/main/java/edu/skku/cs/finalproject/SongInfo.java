package edu.skku.cs.finalproject;


import java.util.ArrayList;

public class SongInfo{
    ArrayList<Object> matches = new ArrayList<Object>();
    private float timestamp;
    private String timezone;
    private String tagid;
    Track track = new Track();


    // Getter Methods

    public float getTimestamp() {
        return timestamp;
    }

    public String getTimezone() {
        return timezone;
    }

    public String getTagid() {
        return tagid;
    }

    public Track getTrack() {
        return track;
    }

    // Setter Methods

    public void setTimestamp( float timestamp ) {
        this.timestamp = timestamp;
    }

    public void setTimezone( String timezone ) {
        this.timezone = timezone;
    }

    public void setTagid( String tagid ) {
        this.tagid = tagid;
    }

    public void setTrack( Track trackObject ) {
        this.track = trackObject;
    }

}
class Track {
    private String layout;
    private String type;
    private String key;
    private String title;
    private String subtitle;
    Images images = new Images();
    Share share = new Share();
    Hub hub = new Hub();
    private String url;
    ArrayList<Object> artists = new ArrayList<Object>();
    private String isrc;
    Genres genres = new Genres();
    Urlparams urlparams = new Urlparams();
    Myshazam MyshazamObject;
    private String albumadamid;
    ArrayList<Object> sections = new ArrayList<Object>();


    // Getter Methods

    public String getLayout() {
        return layout;
    }

    public String getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getTitle() {
        return title;
    }

    public String getSubtitle() {
        return subtitle;
    }

    public Images getImages() {
        return images;
    }

    public Share getShare() {
        return share;
    }

    public Hub getHub() {
        return hub;
    }

    public String getUrl() {
        return url;
    }

    public String getIsrc() {
        return isrc;
    }

    public Genres getGenres() {
        return genres;
    }

    public Urlparams getUrlparams() {
        return urlparams;
    }

    public Myshazam getMyshazam() {
        return MyshazamObject;
    }

    public String getAlbumadamid() {
        return albumadamid;
    }

    // Setter Methods

    public void setLayout( String layout ) {
        this.layout = layout;
    }

    public void setType( String type ) {
        this.type = type;
    }

    public void setKey( String key ) {
        this.key = key;
    }

    public void setTitle( String title ) {
        this.title = title;
    }

    public void setSubtitle( String subtitle ) {
        this.subtitle = subtitle;
    }

    public void setImages( Images imagesObject ) {
        this.images = imagesObject;
    }

    public void setShare( Share shareObject ) {
        this.share = shareObject;
    }

    public void setHub( Hub hubObject ) {
        this.hub = hubObject;
    }

    public void setUrl( String url ) {
        this.url = url;
    }

    public void setIsrc( String isrc ) {
        this.isrc = isrc;
    }

    public void setGenres( Genres genresObject ) {
        this.genres = genresObject;
    }

    public void setUrlparams( Urlparams urlparamsObject ) {
        this.urlparams = urlparamsObject;
    }

    public void setMyshazam( Myshazam myshazamObject ) {
        this.MyshazamObject = myshazamObject;
    }

    public void setAlbumadamid( String albumadamid ) {
        this.albumadamid = albumadamid;
    }
}
class Myshazam {
    Apple AppleObject;


    // Getter Methods

    public Apple getApple() {
        return AppleObject;
    }

    // Setter Methods

    public void setApple( Apple appleObject ) {
        this.AppleObject = appleObject;
    }
}
class Apple {
    ArrayList<Object> actions = new ArrayList<Object>();


    // Getter Methods



    // Setter Methods


}
class Urlparams {
    private Object tracktitle;
    private Object trackartist;


    // Getter Methods

    public Object getTracktitle() {
        return tracktitle;
    }

    public Object getTrackartist() {
        return trackartist;
    }

    // Setter Methods

    public void setTracktitle( Object tracktitle ) {
        this.tracktitle = tracktitle;
    }

    public void setTrackartist( Object trackartist ) {
        this.trackartist = trackartist;
    }
}
class Genres {
    private String primary;


    // Getter Methods

    public String getPrimary() {
        return primary;
    }

    // Setter Methods

    public void setPrimary( String primary ) {
        this.primary = primary;
    }
}
class Hub {
    private String type;
    private String image;
    ArrayList<Object> actions = new ArrayList<Object>();
    ArrayList<Object> options = new ArrayList<Object>();
    ArrayList<Object> providers = new ArrayList<Object>();
    private boolean explicit;
    private String displayname;


    // Getter Methods

    public String getType() {
        return type;
    }

    public String getImage() {
        return image;
    }

    public boolean getExplicit() {
        return explicit;
    }

    public String getDisplayname() {
        return displayname;
    }

    // Setter Methods

    public void setType( String type ) {
        this.type = type;
    }

    public void setImage( String image ) {
        this.image = image;
    }

    public void setExplicit( boolean explicit ) {
        this.explicit = explicit;
    }

    public void setDisplayname( String displayname ) {
        this.displayname = displayname;
    }
}
class Share {
    private String subject;
    private String text;
    private String href;
    private String image;
    private String twitter;
    private String html;
    private String avatar;
    private String snapchat;


    // Getter Methods

    public String getSubject() {
        return subject;
    }

    public String getText() {
        return text;
    }

    public String getHref() {
        return href;
    }

    public String getImage() {
        return image;
    }

    public String getTwitter() {
        return twitter;
    }

    public String getHtml() {
        return html;
    }

    public String getAvatar() {
        return avatar;
    }

    public String getSnapchat() {
        return snapchat;
    }

    // Setter Methods

    public void setSubject( String subject ) {
        this.subject = subject;
    }

    public void setText( String text ) {
        this.text = text;
    }

    public void setHref( String href ) {
        this.href = href;
    }

    public void setImage( String image ) {
        this.image = image;
    }

    public void setTwitter( String twitter ) {
        this.twitter = twitter;
    }

    public void setHtml( String html ) {
        this.html = html;
    }

    public void setAvatar( String avatar ) {
        this.avatar = avatar;
    }

    public void setSnapchat( String snapchat ) {
        this.snapchat = snapchat;
    }
}
class Images {
    private String background;
    private String coverart;
    private String coverarthq;
    private String joecolor;


    // Getter Methods

    public String getBackground() {
        return background;
    }

    public String getCoverart() {
        return coverart;
    }

    public String getCoverarthq() {
        return coverarthq;
    }

    public String getJoecolor() {
        return joecolor;
    }

    // Setter Methods

    public void setBackground( String background ) {
        this.background = background;
    }

    public void setCoverart( String coverart ) {
        this.coverart = coverart;
    }

    public void setCoverarthq( String coverarthq ) {
        this.coverarthq = coverarthq;
    }

    public void setJoecolor( String joecolor ) {
        this.joecolor = joecolor;
    }
}
