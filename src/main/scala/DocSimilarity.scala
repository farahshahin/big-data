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
      // Read stopwords and broadcast them
      val stopWords = sc.textFile(stopwordsPath)
        .map(_.trim.toLowerCase(Locale.ROOT))
        .filter(_.nonEmpty)
        .collect()
        .toSet

      val stopWordsBc = sc.broadcast(stopWords)

      // Task 1: Read documents as (fileName, content)
      val docs = sc.wholeTextFiles(inputDir)
        .map {
          case (path, content) =>
            (new Path(path).getName, content)
        }
        .cache()

      // Task 2: Preprocess documents
      val tokens = docs.mapValues { content =>
        content
          .toLowerCase(Locale.ROOT)
          .split("\\s+")
          .iterator
          .filter(_.nonEmpty)
          .filterNot(stopWordsBc.value.contains)
          .filter(_.length > 1)
          .toList
      }.cache()

      // Print token lists for two selected documents
      println("=== Preprocessed Token Lists ===")

      val selectedTokens = tokens
        .filter { case (name, _) =>
          name == "doc01.txt" || name == "doc02.txt"
        }
        .collect()
        .sortBy(_._1)

      selectedTokens.foreach { case (name, words) =>
        println(s"$name: ${words.mkString("[", ", ", "]")}")
      }

      // Task 3: Convert token lists to sets
      val tokenSets = tokens.mapValues(_.toSet).cache()

      // Generate unique document pairs and calculate Jaccard scores
      val scores = tokenSets
        .cartesian(tokenSets)
        .filter {
          case ((d1, _), (d2, _)) => d1 < d2
        }
        .map {
          case ((d1, setA), (d2, setB)) =>
            (d1, d2, jaccard(setA, setB))
        }

      // Task 4: Select top 20 scores
      // Highest score first, then d1 and d2 ascending
      val ordering =
        Ordering.by[(String, String, Double),
          (Double, String, String)] {
          case (d1, d2, score) =>
            (-score, d1, d2)
        }

      val top20 = scores.takeOrdered(20)(ordering)

      // Format scores to four decimal places
      val output = top20.map {
        case (d1, d2, score) =>
          f"$d1,$d2,$score%.4f"
      }

      // Print the complete top 20 results
      println("=== Top 20 Similar Document Pairs ===")
      output.foreach(println)

      // Save results as a single part file
      sc.parallelize(output.toSeq, 1)
        .saveAsTextFile(outputDir)

      println(s"Results saved to: $outputDir")

    } finally {
      sc.stop()
    }
  }
}
