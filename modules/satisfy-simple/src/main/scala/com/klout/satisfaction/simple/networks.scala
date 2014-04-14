package com.klout
package satisfy
package track
package simple

case class Network(featureGroup: Int, networkAbbr: String, networkFull: String)

object Networks {

    def Klout = Network(0, "kl", "klout")

    def Twitter = Network(1, "tw", "twitter")

    def Facebook = Network(2, "fb", "facebook")

    def LinkedIn = Network(3, "li", "linkedin")

    def Foursquare = Network(4, "fs", "foursquare")

    def Instagram = Network(6, "ig", "instagram")

    def FacebookPages = Network(11, "fp", "facebook_pages")

    def GooglePlus = Network(13, "gp", "google_plus")

    def Yammer = Network(15, "ya", "yammer")

    def Classifier = Network(100, "df", "classifier")

    def Wikipedia = Network(101, "wk", "wikipedia")

    def Bing = Network(102, "bi", "bing")

    def Yelp = Network(501, "yl", "yelp")
}