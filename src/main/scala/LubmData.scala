/**
  * Created by xiangnanren on 23/06/16.
  */
trait LubmData

case class LubmSameAs(file:String){
  val sameAsFile: String = file
}

case class LubmTransitive(file:String){
  val transitiveFile: String = file
}
