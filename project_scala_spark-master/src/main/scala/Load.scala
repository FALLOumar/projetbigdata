
import org.apache.spark.sql.{DataFrame, SaveMode}

object Load {

  /**
   * Cette fonction permet de sauvegarder les données dans un répertoire local
   * @param df : DataFrame
   * @param saveMode : méthode de sauvegarde (overwrite, append, etc.)
   * @param format : format des fichiers de sauvegarde (parquet, csv, etc.)
   * @param path : chemin où les données doivent être sauvegardées
   * @return : Nothing
   */
  def saveData(df: DataFrame, saveMode: String, format: String, path: String): Unit = {
    try {
      df.write
        .mode(SaveMode.valueOf(saveMode.toUpperCase))
        .format(format)
        .save(path)
    } catch {
      case e: Exception =>
        println(s"Erreur lors de la sauvegarde des données : ${e.getMessage}")
        throw e
    }
  }
}
