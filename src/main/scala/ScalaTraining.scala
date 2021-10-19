import java.util.Properties


object ScalaTraining {

  def main(args : Array[String]) : Unit = {
    val mavar : String = "Hello World"
    println(mavar)
    val mavar1 : String = mavar + "hello world"
    val test = 1

    extractFirst(mavar,3)
    val newText : String = extractFirstFonction(mavar,3)
    print(newText)

    var i=0

  /*  while (i<10){
      println(s"voici la valeur de ${i}")
      i=i+1
    }*/

   /* for(i<- 1 to 10){
      println(s"voici la valeur de ${i}")
    }*/

    //LIST

    val maListe : List[String] = List("anthony", "taylor", "juvenal")
    val ml =maListe.foreach {
      e=> {
        val t = e.substring(1,2)
        val result ="15"+t
        println(result)
      }
    }

    val maListe2 =maListe.map(m=>m.substring(1,2)) // transormation
    val maListe3 = maListe2.filter(f=>f=="a")

    val intList : List[Int] = List (10,48,89,100,46)
    val intList2 = intList.map(_*3)
    intList2.foreach{e=> println(e)}

    val tp : (String, Int, Boolean, String)=("juvenal", 10 , true, "adrien")
    val texte = tp._4

    //MAP
    val map1 : Map[String, String]= Map("adresseIP"->"127.0.0.1","hostName"->"Juvenal", "portNumber"->"3392")
    val map2 : Map[String, Int]= Map("distance"->100,"distance"->20, "distance"->25)
    val map3 : Map[String, List[String]]= Map("villes" ->List("Paris", "Tokyo"), "pays"->List("France", "Japon"))

    map1.keys.foreach{k=> println(s"cles de mon map1 : ${k}")}

    val cle1=map1("adresseIP")
    println(cle1)

    val valmap3 = map3.values.foreach{s=> {
      s.foreach{y=>println(s"valeur map 3 : ${y}")}}}

    println(valmap3)

    // TABLEAU

    val monTableau : Array[String] = Array("juvenal", "anthony", "bruno")
    monTableau(0)
    val listeTableau = monTableau.toList





  }



  def extractFirst (texte : String, longueur : Int) : Unit={
    val resultat = texte.substring(longueur)
    println("votre texte extrait est : "+resultat)
  }


  def extractFirstFonction (texte : String, longueur : Int) : String={
    val resultat = texte.substring(longueur)
    return resultat
  }
}
