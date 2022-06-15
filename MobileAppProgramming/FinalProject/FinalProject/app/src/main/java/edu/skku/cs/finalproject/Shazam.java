package edu.skku.cs.finalproject;

import androidx.annotation.NonNull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class Shazam
{
    String test = "{\n" +
            "\"matches\": [\n" +
            "{\n" +
            "\"id\": \"271567832\",\n" +
            "\"offset\": 2.2199260250000004,\n" +
            "\"channel\": \"0\",\n" +
            "\"timeskew\": -0.0011238456,\n" +
            "\"frequencyskew\": 0.00023019314\n" +
            "},\n" +
            "{\n" +
            "\"id\": \"228714213\",\n" +
            "\"offset\": 2.124595458,\n" +
            "\"channel\": \"0\",\n" +
            "\"timeskew\": -0.0007727742,\n" +
            "\"frequencyskew\": 0.00004029274\n" +
            "},\n" +
            "{\n" +
            "\"id\": \"48505807\",\n" +
            "\"offset\": 21.258058593,\n" +
            "\"channel\": \"0\",\n" +
            "\"timeskew\": -0.0021508336,\n" +
            "\"frequencyskew\": 0.00096702576\n" +
            "}\n" +
            "],\n" +
            "\"timestamp\": 1636371123342,\n" +
            "\"timezone\": \"America/Chicago\",\n" +
            "\"tagid\": \"e676d6f1-3d7e-4a73-9041-84024315d83a\",\n" +
            "\"track\": {\n" +
            "\"layout\": \"5\",\n" +
            "\"type\": \"MUSIC\",\n" +
            "\"key\": \"5167035\",\n" +
            "\"title\": \"Clint Eastwood\",\n" +
            "\"subtitle\": \"Gorillaz\",\n" +
            "\"images\": {\n" +
            "\"background\": \"https://is4-ssl.mzstatic.com/image/thumb/Features115/v4/7e/45/a8/7e45a8e5-8f28-9bbd-f6d0-9524dba7a9fe/prsource.png/800x800cc.jpg\", \"coverart\": \"https://is2-ssl.mzstatic.com/image/thumb/Music115/v4/5b/8d/47/5b8d47da-71ea-93ab-dffc-733f11332659/825646290703.jpg/400x400cc.jpg\", \"coverarthq\": \"https://is2-ssl.mzstatic.com/image/thumb/Music115/v4/5b/8d/47/5b8d47da-71ea-93ab-dffc-733f11332659/825646290703.jpg/400x400cc.jpg\", \"joecolor\": \"b:ffffffp:040705s:172b18t:363837q:455546\" }, \"share\": { \"subject\": \"Clint Eastwood - Gorillaz\", \"text\": \"I used Shazam to discover Clint Eastwood by Gorillaz.\", \"href\": \"https://www.shazam.com/track/5167035/clint-eastwood\", \"image\": \"https://is2-ssl.mzstatic.com/image/thumb/Music115/v4/5b/8d/47/5b8d47da-71ea-93ab-dffc-733f11332659/825646290703.jpg/400x400cc.jpg\", \"twitter\": \"I used @Shazam to discover Clint Eastwood by Gorillaz.\", \"html\": \"https://www.shazam.com/snippets/email-share/5167035?lang=en-US&country=US\", \"avatar\": \"https://is4-ssl.mzstatic.com/image/thumb/Features115/v4/7e/45/a8/7e45a8e5-8f28-9bbd-f6d0-9524dba7a9fe/prsource.png/800x800cc.jpg\",\n" +
            "\"snapchat\": \"https://www.shazam.com/partner/sc/track/5167035\"\n" +
            "},\n" +
            "\"hub\": {\n" +
            "\"type\": \"APPLEMUSIC\",\n" +
            "\"image\": \"https://images.shazam.com/static/icons/hub/ios/v5/applemusic{scalefactor}.png\", \"actions\": [ { \"name\": \"apple\", \"type\": \"applemusicplay\", \"id\": \"850576665\" }, { \"name\": \"apple\", \"type\": \"uri\", \"uri\": \"https://audio-ssl.itunes.apple.com/itunes-assets/AudioPreview115/v4/90/22/95/902295fa-d5e5-4ea9-dfd6-26a5417d42b0/mzaf13068135983766007631.plus.aac.ep.m4a\"\n" +
            "}\n" +
            "],\n" +
            "\"options\": [\n" +
            "{\n" +
            "\"caption\": \"OPEN\",\n" +
            "\"actions\": [\n" +
            "{\n" +
            "\"name\": \"hub:applemusic:deeplink\",\n" +
            "\"type\": \"applemusicopen\",\n" +
            "\"uri\": \"https://music.apple.com/us/album/clint-eastwood/850576570?i=850576665&mttnagencyid=769459046716559743&mttnsiteid=125115&mttn3pid=acustom779816081798873874&mttnsub1=Shazamios&mttnsub2=5348615A-616D-3235-3830-44754D6D5973&itscg=30201&app=music&itsct=Shazamios\"\n" +
            "},\n" +
            "{\n" +
            "\"name\": \"hub:applemusic:deeplink\",\n" +
            "\"type\": \"uri\",\n" +
            "\"uri\": \"https://music.apple.com/us/album/clint-eastwood/850576570?i=850576665&mttnagencyid=769459046716559743&mttnsiteid=125115&mttn3pid=acustom779816081798873874&mttnsub1=Shazamios&mttnsub2=5348615A-616D-3235-3830-44754D6D5973&itscg=30201&app=music&itsct=Shazamios\"\n" +
            "}\n" +
            "],\n" +
            "\"beacondata\": {\n" +
            "\"type\": \"open\",\n" +
            "\"providername\": \"applemusic\"\n" +
            "},\n" +
            "\"image\": \"https://images.shazam.com/static/icons/hub/ios/v5/overflow-open-option{scalefactor}.png\", \"type\": \"open\", \"listcaption\": \"Open in Apple Music\", \"overflowimage\": \"https://images.shazam.com/static/icons/hub/ios/v5/applemusic-overflow{scalefactor}.png\",\n" +
            "\"colouroverflowimage\": false,\n" +
            "\"providername\": \"applemusic\"\n" +
            "},\n" +
            "{\n" +
            "\"caption\": \"BUY\",\n" +
            "\"actions\": [\n" +
            "{\n" +
            "\"type\": \"uri\",\n" +
            "\"uri\": \"https://itunes.apple.com/us/album/clint-eastwood/850576570?i=850576665&mttnagencyid=769459046716559743&mttnsiteid=125115&mttn3pid=acustom779816081798873874&mttnsub1=Shazamios&mttnsub2=5348615A-616D-3235-3830-44754D6D5973&itscg=30201&app=itunes&itsct=Shazamios\"\n" +
            "}\n" +
            "],\n" +
            "\"beacondata\": {\n" +
            "\"type\": \"buy\",\n" +
            "\"providername\": \"itunes\"\n" +
            "},\n" +
            "\"image\": \"https://images.shazam.com/static/icons/hub/ios/v5/itunes-overflow-buy{scalefactor}.png\", \"type\": \"buy\", \"listcaption\": \"Buy on iTunes\", \"overflowimage\": \"https://images.shazam.com/static/icons/hub/ios/v5/itunes-overflow-buy{scalefactor}.png\",\n" +
            "\"colouroverflowimage\": false,\n" +
            "\"providername\": \"itunes\"\n" +
            "}\n" +
            "],\n" +
            "\"providers\": [\n" +
            "{\n" +
            "\"caption\": \"Open in Spotify\",\n" +
            "\"images\": {\n" +
            "\"overflow\": \"https://images.shazam.com/static/icons/hub/ios/v5/spotify-overflow{scalefactor}.png\", \"default\": \"https://images.shazam.com/static/icons/hub/ios/v5/spotify{scalefactor}.png\"\n" +
            "},\n" +
            "\"actions\": [\n" +
            "{\n" +
            "\"name\": \"hub:spotify:searchdeeplink\",\n" +
            "\"type\": \"uri\",\n" +
            "\"uri\": \"spotify:search:Clint%20Eastwood%20Gorillaz\"\n" +
            "}\n" +
            "],\n" +
            "\"type\": \"SPOTIFY\"\n" +
            "},\n" +
            "{\n" +
            "\"caption\": \"Open in Deezer\",\n" +
            "\"images\": {\n" +
            "\"overflow\": \"https://images.shazam.com/static/icons/hub/ios/v5/deezer-overflow{scalefactor}.png\", \"default\": \"https://images.shazam.com/static/icons/hub/ios/v5/deezer{scalefactor}.png\"\n" +
            "},\n" +
            "\"actions\": [\n" +
            "{\n" +
            "\"name\": \"hub:deezer:searchdeeplink\",\n" +
            "\"type\": \"uri\",\n" +
            "\"uri\": \"deezer-query://www.deezer.com/play?query=%7Btrack%3A%27Clint+Eastwood%27%20artist%3A%27Gorillaz%27%7D\"\n" +
            "}\n" +
            "],\n" +
            "\"type\": \"DEEZER\"\n" +
            "}\n" +
            "],\n" +
            "\"explicit\": true,\n" +
            "\"displayname\": \"APPLE MUSIC\"\n" +
            "},\n" +
            "\"url\": \"https://www.shazam.com/track/5167035/clint-eastwood\",\n" +
            "\"artists\": [\n" +
            "{\n" +
            "\"id\": \"352573\",\n" +
            "\"adamid\": \"567072\"\n" +
            "}\n" +
            "],\n" +
            "\"isrc\": \"GBAYE1400468\",\n" +
            "\"genres\": {\n" +
            "\"primary\": \"Alternative\"\n" +
            "},\n" +
            "\"urlparams\": {\n" +
            "\"{tracktitle}\": \"Clint+Eastwood\",\n" +
            "\"{trackartist}\": \"Gorillaz\"\n" +
            "},\n" +
            "\"myshazam\": {\n" +
            "\"apple\": {\n" +
            "\"actions\": [\n" +
            "{\n" +
            "\"name\": \"myshazam:apple\",\n" +
            "\"type\": \"uri\",\n" +
            "\"uri\": \"https://music.apple.com/us/album/clint-eastwood/850576570?i=850576665&mttnagencyid=769459046716559743&mttnsiteid=125115&mttn3pid=acustom779816081798873874&mttnsub1=Shazamios&mttnsub2=5348615A-616D-3235-3830-44754D6D5973&itscg=30201&app=music&itsct=Shazamios\"\n" +
            "}\n" +
            "]\n" +
            "}\n" +
            "},\n" +
            "\"albumadamid\": \"850576570\",\n" +
            "\"sections\": [\n" +
            "{\n" +
            "\"type\": \"SONG\",\n" +
            "\"metapages\": [\n" +
            "{\n" +
            "\"image\": \"https://is4-ssl.mzstatic.com/image/thumb/Features115/v4/7e/45/a8/7e45a8e5-8f28-9bbd-f6d0-9524dba7a9fe/prsource.png/800x800cc.jpg\", \"caption\": \"Gorillaz\" }, { \"image\": \"https://is2-ssl.mzstatic.com/image/thumb/Music115/v4/5b/8d/47/5b8d47da-71ea-93ab-dffc-733f11332659/825646290703.jpg/400x400cc.jpg\", \"caption\": \"Clint Eastwood\" } ], \"tabname\": \"Song\", \"metadata\": [ { \"title\": \"Album\", \"text\": \"Gorillaz\" }, { \"title\": \"Label\", \"text\": \"Parlophone UK\" }, { \"title\": \"Released\", \"text\": \"2001\" } ] }, { \"type\": \"LYRICS\", \"text\": [ \"I ain't happy, I'm feeling glad\", \"I got sunshine in a bag\", \"I'm useless but not for long\", \"The future is coming on\", \"\", \"I ain't happy, I'm feeling glad\", \"I got sunshine in a bag\", \"I'm useless but not for long\", \"The future is coming on\", \"It's coming on, it's coming on\", \"It's coming on, it's coming on\", \"\", \"Finally someone let me out of my cage\", \"Now time for me is nothin' 'cos I'm counting no age\", \"Now I couldn't be there now you shouldn't be scared\", \"I'm good at repairs and I'm under each snare\", \"\", \"Intangible (Ah y'all), bet you didn't think so\", \"I command you to, panoramic view (You)\", \"Look I'll make it all manageable\", \"Pick and choose, sit and lose\", \"All you different crews\", \"Chicks and dudes, who you think is really kicking tunes\", \"\", \"Picture you getting down and I'll picture too\", \"Like you lit the fuse\", \"You think it's fictional, mystical - maybe\", \"Spiritual hero who appears on you to clear your view\", \"When you're too crazy\", \"\", \"Lifeless to those a definition for what life is\", \"Priceless to you because I put ya on the hype shift\", \"Did ya like it?\", \"Gut smokin' righteous but one talkin' psychic\", \"Among knows possess you with one though\", \"\", \"I ain't happy, I'm feeling glad\", \"I got sunshine in a bag\", \"I'm useless but not for long\", \"The future is coming on\", \"\", \"I ain't happy, I'm feeling glad\", \"I got sunshine in a bag\", \"I'm useless but not for long\", \"The future (That's right) is coming on\", \"It's coming on, it's coming on, it's coming on, it's coming on\", \"\", \"The essence, the basics without it you make it\", \"Allow me to make this child like in nature\", \"Rhythm you have it or you don't\", \"That's a fallacy, I'm in them\", \"Every spiralling tree, every child of peace\", \"Every cloud I see you see with your eyes\", \"\", \"I see destruction and demise\", \"Corruption in the skies\", \"From this fucking enterprise that I'm sucked into your lies\", \"The Russell that is muscles\", \"But percussion he provides\", \"\", \"For me I say God, y'all can see me now\", \"'Cos you don't see with your eye\", \"You perceive with your mind\", \"That's the end of it\", \"So I'mma stick around with Russ and be a mentor\", \"\", \"Bust a few rounds on motherfuckers\", \"Remember what the thought is\", \"I brought all this so you can survive when law is lawless (Right here)\", \"Fearless, sensations that you thought was dead\", \"No squealing, remember that it's all in your head\", \"\", \"I ain't happy, I'm feeling glad\", \"I got sunshine in a bag\", \"I'm useless but not for long\", \"The future is coming on\", \"\", \"I ain't happy, I'm feeling glad\", \"I got sunshine in a bag\", \"I'm useless but not for long\", \"My future is coming on\", \"It's coming on, it's coming on, it's coming on, it's coming on\", \"\", \"My future\", \"It's coming on, it's coming on, it's coming on\", \"It's coming on, it's coming on, my future\", \"It's coming on, it's coming on, it's coming on\", \"It's coming on, it's coming on, my future\", \"It's coming on, it's coming on, it's coming on\", \"\", \"My future\", \"It's coming on, it's coming on, it's coming on\", \"My future\", \"It's coming on, it's coming on, it's coming on\", \"My future\" ], \"footer\": \"Writer(s): Damon Albarn, Teron Delvon Jones, Murdoc Niccals\\nLyrics powered by www.musixmatch.com\", \"tabname\": \"Lyrics\", \"beacondata\": { \"lyricsid\": \"19915415\", \"providername\": \"musixmatch\", \"commontrackid\": \"1149210\" } }, { \"type\": \"VIDEO\", \"tabname\": \"Video\", \"youtubeurl\": { \"caption\": \"Gorillaz - Clint Eastwood (Official Video)\", \"image\": { \"dimensions\": { \"width\": 1280, \"height\": 720 }, \"url\": \"https://i.ytimg.com/vi/1VxRb0x9aw/maxresdefault.jpg\"\n" +
            "},\n" +
            "\"actions\": [\n" +
            "{\n" +
            "\"name\": \"video:youtube\",\n" +
            "\"type\": \"webview\",\n" +
            "\"share\": {\n" +
            "\"subject\": \"Clint Eastwood - Gorillaz\",\n" +
            "\"text\": \"I used Shazam to discover Clint Eastwood by Gorillaz.\",\n" +
            "\"href\": \"https://www.shazam.com/track/5167035/clint-eastwood\",\n" +
            "\"image\": \"https://is2-ssl.mzstatic.com/image/thumb/Music115/v4/5b/8d/47/5b8d47da-71ea-93ab-dffc-733f11332659/825646290703.jpg/400x400cc.jpg\",\n" +
            "\"twitter\": \"I used @Shazam to discover Clint Eastwood by Gorillaz.\",\n" +
            "\"html\": \"https://www.shazam.com/snippets/email-share/5167035?lang=-&country=US\",\n" +
            "\"avatar\": \"https://is4-ssl.mzstatic.com/image/thumb/Features115/v4/7e/45/a8/7e45a8e5-8f28-9bbd-f6d0-9524dba7a9fe/prsource.png/800x800cc.jpg\", \"snapchat\": \"https://www.shazam.com/partner/sc/track/5167035\" }, \"uri\": \"https://youtu.be/1VxRb0x9aw?autoplay=1\"\n" +
            "}\n" +
            "]\n" +
            "}\n" +
            "},\n" +
            "{\n" +
            "\"type\": \"ARTIST\",\n" +
            "\"avatar\": \"https://is4-ssl.mzstatic.com/image/thumb/Features115/v4/7e/45/a8/7e45a8e5-8f28-9bbd-f6d0-9524dba7a9fe/pr_source.png/800x800cc.jpg\",\n" +
            "\"id\": \"352573\",\n" +
            "\"name\": \"Gorillaz\",\n" +
            "\"verified\": false,\n" +
            "\"actions\": [\n" +
            "{\n" +
            "\"type\": \"artistposts\",\n" +
            "\"id\": \"352573\"\n" +
            "},\n" +
            "{\n" +
            "\"type\": \"artist\",\n" +
            "\"id\": \"352573\"\n" +
            "}\n" +
            "],\n" +
            "\"tabname\": \"Artist\"\n" +
            "},\n" +
            "{\n" +
            "\"type\": \"RELATED\",\n" +
            "\"tabname\": \"Related\"\n" +
            "}\n" +
            "]\n" +
            "}\n" +
            "}";

    private SongInfo songInfo;

    public void requestShazam(ViewModel viewModel, String value){
/*
        Gson gson = new GsonBuilder().create();
        songInfo = gson.fromJson(test, SongInfo.class);
        viewModel.getDetectedSong().postValue(songInfo);

 */


        OkHttpClient client = new OkHttpClient();

        MediaType mediaType = MediaType.parse("text/plain");
        RequestBody body = RequestBody.create(mediaType, value);
        Request request = new Request.Builder()
                .url("https://shazam.p.rapidapi.com/songs/detect")
                .post(body)
                .addHeader("content-type", "text/plain")
                .addHeader("X-RapidAPI-Host", "shazam.p.rapidapi.com")
                .addHeader("X-RapidAPI-Key", "921158daf4msh34cc5e645a62be4p1a5205jsnaa865827720c")
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(@NonNull Call call, @NonNull IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(@NonNull Call call, @NonNull Response response) throws IOException {
                final String myResponse = response.body().string();

                Gson gson = new GsonBuilder().create();
                songInfo = gson.fromJson(myResponse, SongInfo.class);
                if(songInfo.matches.size()!=0){
                    viewModel.setSong(songInfo.track.getTitle());
                    viewModel.getDetectedSong().postValue(songInfo);
                    History.tempShazam = myResponse;
                }else{
                    viewModel.setSong("No match found");

                }
            }
        });


    }

}
