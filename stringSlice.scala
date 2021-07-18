import scala.io.Source
import java.io._

object stringSlice {
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    val fixed_file = Source.fromFile("fixed_width_data.txt")
    val result_sink = new File("results.txt")
    val bw = new BufferedWriter(new FileWriter(result_sink))
    for (line <- fixed_file.getLines){
      val (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) = slice_values(line)
      bw.write(s"$c1, $c2, $c3, $c4, $c5, $c6, $c7, $c8, $c9, $c10, $c11, $c12, $c13 \n")
    }
    bw.close()
    val duration = (System.nanoTime - t1) / 1e9d
    println(duration)
  }

  def slice_values(line: String): (String,String,String,String,String,String,String,String,String,String,String,String,String) = {
    val c1 = line.slice(0, 45).trim()
    val c2 = line.slice(45, 90).trim()
    val c3 = line.slice(90, 135).trim()
    val c4 = line.slice(135, 180).trim()
    val c5 = line.slice(180, 225).trim()
    val c6 = line.slice(225, 270).trim()
    val c7 = line.slice(270, 315).trim()
    val c8 = line.slice(315, 360).trim()
    val c9 = line.slice(360, 405).trim()
    val c10 = line.slice(405, 450).trim()
    val c11 = line.slice(450, 495).trim()
    val c12 = line.slice(495, 540).trim()
    val c13 = line.slice(540, 585).trim()
    (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)
  }
}