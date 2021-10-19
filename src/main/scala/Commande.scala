case class Commande(
                   facture : SchemaFacture,
                   produitCommande : DetailFacture
                   )
{
  def revenueCompute(quantite : Int, prixUnite : Double) : Double= {
  return quantite * prixUnite
  }

  def taxesCompute(tauxtva : Double, totalFacture : Double) : Double ={
    return tauxtva * totalFacture
  }


}
