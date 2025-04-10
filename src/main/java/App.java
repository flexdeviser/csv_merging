import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

public class App {


    public static void main(String[] args) {

        ExecutorService runService = Executors.newFixedThreadPool(4);

        // loop through folders to pass 3 files on each
        File src = new File("/Users/ericwang/Downloads/Processed Synthetic Data ");
        File[] folders = src.listFiles();
        for (int i = 0; i < folders.length; i++) {
            if (folders[i].isDirectory()) {
                int finalI = i;
                runService.submit(() -> {
                    generateCsv(folders[finalI].toPath(), folders[finalI].getName());
                });
            }
        }

        // wait for done
        runService.shutdown();
    }


    private static void generateCsv(Path folder, String txName) {
        Map<String, List<String[]>> result = new LinkedHashMap<>();

        System.out.println("starting - " + txName + " at: " + new Date());
        try {

            InputStream headerIn = new FileInputStream(folder + "/V.csv");

            CSVReader headerReader = new CSVReader(new InputStreamReader(headerIn));
            // find all devices
            String[] header = headerReader.readNext();
            List<String> devices = new ArrayList<>();
            // 1 ~ length - 1
            for (int i = 1; i < header.length; i++) {
                String device = header[i].substring(header[i].indexOf(".") + 4);
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

            // channel mapping
            String[] vHeader = vReader.readNext();
            String[] pHeader = vReader.readNext();
            String[] qHeader = vReader.readNext();

            CSVWriter writer = new CSVWriter(new FileWriter("/Users/ericwang/Workspace/csv/" + txName + "_data.csv"),
                                             CSVWriter.DEFAULT_SEPARATOR,
                                             CSVWriter.NO_QUOTE_CHARACTER,
                                             CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                                             CSVWriter.RFC4180_LINE_END);
            // new csv file headers, include v p q and 3 different channels
            String[] newHeader = {"device", "timeKey", "vA", "vB", "vC", "pA", "pB", "pC", "qA", "qB", "qC"};
            writer.writeNext(newHeader);

            boolean c = true;

            while (c) {
                // read voltage first
                fillingData(txName, vReader, devices, result, "V");

                // read p
                fillingData(txName, pReader, devices, result, "P");

                // read q last
                boolean more = fillingData(txName, qReader, devices, result, "Q");

                // write data to new csv file
                result.forEach((k, v) -> {
                    writer.writeAll(v, false);
                });

                // cleanup memory
                result = new HashMap<>();

                // init new arraylist for each device
                for (String device : devices) {
                    result.put(device, new ArrayList<>());
                }

                if (!more) {
                    c = false;
                }
            }

            // close resources
            writer.close();
            vReader.close();
            pReader.close();
            qReader.close();

        } catch (CsvValidationException | IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("finished - " + txName + " at: " + new Date());
    }


    private static boolean fillingData(String txName, CSVReader reader, List<String> devices,
                                       Map<String, List<String[]>> result, String type)
        throws CsvValidationException, IOException {
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
                // find channel
                String[] finalNextLine = nextLine;
                Optional<String[]> exist = dtos.stream().filter(dto -> finalNextLine[0].equals(dto[1])).findFirst();
                String[] k;
                String deviceName = devices.get(i);
                String channel = deviceName.substring(deviceName.indexOf(".") + 1);
                //  String[] newHeader = {"device", "timeKey", "vA", "vB", "vC", "pA", "pB", "pC", "qA", "qB", "qC"};
                if (!exist.isEmpty()) {
                    k = exist.get();
                } else {
                    // not found. create one
                    k = new String[11];
                    k[0] = deviceName.substring(0, deviceName.indexOf("."));
                    k[1] = (finalNextLine[0]);
                    dtos.add(k);
                }
                switch (type) {
                    case "V":
                        if (channel.equals("A")) {
                            k[2] = finalNextLine[i + 1];
                        } else if (channel.equals("B")) {
                            k[3] = finalNextLine[i + 1];
                        } else if (channel.equals("C")) {
                            k[4] = finalNextLine[i + 1];
                        }

                        break;
                    case "P":
                        if (channel.equals("A")) {
                            k[5] = finalNextLine[i + 1];
                        } else if (channel.equals("B")) {
                            k[6] = finalNextLine[i + 1];
                        } else if (channel.equals("C")) {
                            k[7] = finalNextLine[i + 1];
                        }
                        break;
                    case "Q":
                        if (channel.equals("A")) {
                            k[8] = finalNextLine[i + 1];
                        } else if (channel.equals("B")) {
                            k[9] = finalNextLine[i + 1];
                        } else if (channel.equals("C")) {
                            k[10] = finalNextLine[i + 1];
                        }
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
            System.out.println("TX: " + txName + "finished: " + rowNumber);
        }

        return more;
    }


}
