package edu.ensias.hadoop.hdfslab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        // Vérifier qu'un paramètre a été fourni
        if (args.length < 1) {
            System.err.println("Usage: hadoop jar ReadHDFS.jar <chemin_du_fichier_HDFS>");
            System.exit(1);
        }

        // Le chemin du fichier HDFS à lire
        String hdfsFilePath = args[0];

        // Configuration Hadoop
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(hdfsFilePath);

        // Vérifie si le fichier existe
        if (!fs.exists(filePath)) {
            System.out.println("Le fichier n'existe pas : " + hdfsFilePath);
            fs.close();
            return;
        }

        // Lecture du fichier
        try (FSDataInputStream inStream = fs.open(filePath);
             BufferedReader br = new BufferedReader(new InputStreamReader(inStream))) {

            System.out.println("Contenu du fichier : " + hdfsFilePath);
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        }

        fs.close();
    }
}
