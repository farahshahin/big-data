
package com.example.docsimilarity

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.Path
import java.util.Locale

object DocSimilarity {

  // Calculate Jaccard Similarity
  def jaccard(a: Set[String], b: Set[String]): Double = {
    val intersection = a.intersect(b).size
    val union = a.union(b).size

    if (union == 0) 0.0
    else intersection.toDouble / union
  }

  def main(args: Array[String]): Unit = {

    require(
      args.length == 3,
      "Usage: DocSimilarity <inputDir> <stopwordsPath> <outputDir>"
    )

    val Array(inputDir, stopwordsPath, outputDir) = args

    val conf = new SparkConf()
      .setAppName("DocSimilarity")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    try {
      // Read stopwords and create a Set
      val stopWords = sc.textFile(stopwordsPath)
        .map(_.trim.toLowerCase(Locale.ROOT))
        .filter(_.nonEmpty)
        .collect()
        .toSet

      val stopWordsBc = sc.broadcast(stopWords)

      // Task 1: RDD[(fileName, content)]
      val docs = sc.wholeTextFiles(inputDir)
        .map {
          case (path, content) =>
            (new Path(path).getName, content)
        }
        .cache()

      // Task 2: RDD[(fileName, List[token])]
      val tokens = docs.mapValues { content =>
        content
          .toLowerCase(Locale.ROOT)
          .split("\\W+")
          .iterator
          .filter(_.nonEmpty)
          .filterNot(stopWordsBc.value.contains)
          .filter(_.length > 1)
          .toList
      }.cache()

      // Task 3: RDD[(d1, d2, score)] with d1 < d2
      val tokenSets = tokens.mapValues(_.toSet).cache()

      val scores = tokenSets
        .cartesian(tokenSets)
        .filter {
          case ((d1, _), (d2, _)) => d1 < d2
        }
        .map {
          case ((d1, setA), (d2, setB)) =>
            (d1, d2, jaccard(setA, setB))
        }

      // Task 4: Top 20, highest score first
      val ordering =
        Ordering.by[(String, String, Double),
          (Double, String, String)] {
          case (d1, d2, score) =>
            (-score, d1, d2)
        }

      val top20 = scores.takeOrdered(20)(ordering)

      // Format: d1,d2,score
      val output = top20.map {
        case (d1, d2, score) =>
          f"$d1,$d2,$score%.4f"
      }

      output.foreach(println)

      sc.parallelize(output.toSeq, 1)
        .saveAsTextFile(outputDir)

      println(s"Results saved to: $outputDir")

    } finally {
      sc.stop()
    }
  }
}
