object Main {
  def main(args: Array[String]): Unit = {
    if (args.length <= 1) {
      println("Missing two arguments.")
      println("usage : You have to give the source file path ou the output path")
      System.exit(1)
    }
    //get the path to the JSON file
    val jsonFilePath = args(0)
    val outputPath = args(1)


    val df = Extract.read_source_file(jsonFilePath, "json")
    df.show()

    val cleanDF = Transform.cleanData(df)
    cleanDF.show()

    val transformDF = Transform.computeTrafficRevenue(cleanDF)
    transformDF.show()

    Load.saveData(transformDF, "ovewrite", "parquet", outputPath)

  }
}