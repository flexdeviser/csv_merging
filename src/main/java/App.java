import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class App {


    public static void main(String[] args) {


        // loop through folders to pass 3 files on each
        File src = new File("/Users/e4s/Documents/Processed_Synthetic_Data");
        File[] folders = src.listFiles();
        for (int i = 0; i < folders.length; i++) {
            if (folders[i].isDirectory()) {
                generateCsv(folders[i].toPath(), folders[i].getName());
            }
        }


    }


    private static void generateCsv(Path folder, String txName) {
        Map<String, List<String[]>> result = new LinkedHashMap<>();


        try {

            InputStream headerIn = new FileInputStream(folder + "/V.csv");


            CSVReader headerReader = new CSVReader(new InputStreamReader(headerIn));
            // find all devices
            String[] header = headerReader.readNext();
            List<String> devices = new ArrayList<>();
            // 1 ~ length - 1
            for (int i = 1; i < header.length; i++) {
                String device = header[i].substring(header[i].indexOf(".") + 4, header[i].lastIndexOf("."));
                devices.add(device);
                result.put(device, new ArrayList<>());
            }
            // close reader and input
            headerReader.close();
            headerIn.close();

            InputStream vIn = new FileInputStream(folder + "/V.csv");
            InputStream pIn = new FileInputStream(folder + "/P.csv");
            InputStream qIn = new FileInputStream(folder + "/Q.csv");

            CSVReader vReader = new CSVReader(new InputStreamReader(vIn));
            CSVReader pReader = new CSVReader(new InputStreamReader(pIn));
            CSVReader qReader = new CSVReader(new InputStreamReader(qIn));

            vReader.skip(1);
            pReader.skip(1);
            qReader.skip(1);


            CSVWriter writer = new CSVWriter(new FileWriter("/Users/e4s/Workspace/csv/" + txName + "_data.csv"));

            String[] newHeader = {"device", "timeKey", "v", "p", "q"};
            writer.writeNext(newHeader);

            boolean c = true;

            while (c) {
                fillingData(vReader, devices, result, "V");
                fillingData(pReader, devices, result, "P");
                boolean more = fillingData(qReader, devices, result, "Q");
                // write data to new csv file
                result.forEach((k, v) -> {
                    v.forEach(writer::writeNext);
                });

                result = new HashMap<>();

                for (String device : devices) {
                    result.put(device, new ArrayList<>());
                }

                if (!more) {
                    c = false;
                }
            }

            writer.close();
            vReader.close();
            pReader.close();
            qReader.close();

        } catch (CsvValidationException | IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static boolean fillingData(CSVReader reader, List<String> devices, Map<String, List<String[]>> result, String type) throws CsvValidationException, IOException {
        int pageSize = 10000;

        // filling data with voltage
        String[] nextLine;
        int rowNumber = 0;

        boolean more = false;

        while ((nextLine = reader.readNext()) != null) {
            rowNumber++;

            for (int i = 0; i < devices.size(); i++) {

                // find the data list for current device
                List<String[]> dtos = result.get(devices.get(i));

                String[] finalNextLine = nextLine;
                Optional<String[]> exist = dtos.stream().filter(dto -> finalNextLine[0].equals(dto[1])).findFirst();
                String[] k;
                if (exist.isEmpty()) {
                    // not found. create one
                    k = new String[5];
                    k[0] = (devices.get(i));
                    k[1] = (finalNextLine[0]);
                    dtos.add(k);
                } else {
                    k = exist.get();
                }

                switch (type) {
                    case "V":
                        k[2] = finalNextLine[i + 1];
                        break;
                    case "P":
                        k[3] = finalNextLine[i + 1];
                        break;
                    case "Q":
                        k[4] = finalNextLine[i + 1];
                        break;
                    default:
                        break;
                }

            }

            // do next
            if (rowNumber >= pageSize) {
                more = true;
                break;
            }
        }


        if (type.equals("Q")) {
            System.out.println("finished: " + rowNumber);
        }


        return more;
    }


}
