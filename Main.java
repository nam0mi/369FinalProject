import java.io.*;
import java.util.*;

public class Main {
    private static HashMap<Integer, Team> data = new HashMap<>();
    private static HashMap<Integer, Team> trainingData = new HashMap<>();
    private static HashMap<Integer, Team> testingData = new HashMap<>();
    private static HashMap<Integer, HashMap<Integer, Team>> crossValidationSets = new HashMap<>();
    private static HashMap<String, HashMap<Integer, ArrayList<Double>>>  statPerSeason = new HashMap<>();
    private static HashMap<Integer, Team> curSeasonData = new HashMap<>();
    private static ArrayList<Double> TTavgFgPercent = new ArrayList<>();
    private static ArrayList<Double> TTavgX3pPercent = new ArrayList<>();
    private static ArrayList<Double> TTavgFtPercent = new ArrayList<>();
    private static ArrayList<Double> TTavgTrbPerGame = new ArrayList<>();
    private static ArrayList<Double> TTavgAstPerGame = new ArrayList<>();
    private static ArrayList<Double> TTavgStlPerGame = new ArrayList<>();
    private static ArrayList<Double> TTavgBlkPerGame = new ArrayList<>();
    private static ArrayList<Double> TTavgTovPerGame = new ArrayList<>();
    private static ArrayList<Double> TTavgPtsPerGame = new ArrayList<>();

    public static void main(String[] args) {
        process("Final_Project/data.csv");

        trainingData = createTrainingData();
        testingData = createTestingData();
        crossValidationSets = createCrossValidationSets();
        HashMap<Integer, Double> k_error = performCrossValidation();
        System.out.println(k_error.values());
        System.out.println(calculateTestingError(1));
        //calculate2023Wins();
    }

    public static void process(String filename) {
        try {
            File file = new File(filename);
            Scanner scanner = new Scanner(file);

            // Skip the first line (header row)
            scanner.nextLine();

            statPerSeason.put("avgFgPercent", new HashMap<>());
            statPerSeason.put("avgX3pPercent", new HashMap<>());
            statPerSeason.put("avgFtPercent", new HashMap<>());
            statPerSeason.put("avgTrbPerGame", new HashMap<>());
            statPerSeason.put("avgAstPerGame", new HashMap<>());
            statPerSeason.put("avgStlPerGame", new HashMap<>());
            statPerSeason.put("avgBlkPerGame", new HashMap<>());
            statPerSeason.put("avgTovPerGame", new HashMap<>());
            statPerSeason.put("avgPtsPerGame", new HashMap<>());

            Integer counter = 0;

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] values = line.split(",");

                int season = Integer.parseInt(values[0]);
                if(!statPerSeason.get("avgFgPercent").containsKey(season)){
                    statPerSeason.get("avgFgPercent").put(season, new ArrayList<>());
                    statPerSeason.get("avgX3pPercent").put(season, new ArrayList<>());
                    statPerSeason.get("avgFtPercent").put(season, new ArrayList<>());
                    statPerSeason.get("avgTrbPerGame").put(season, new ArrayList<>());
                    statPerSeason.get("avgAstPerGame").put(season, new ArrayList<>());
                    statPerSeason.get("avgStlPerGame").put(season, new ArrayList<>());
                    statPerSeason.get("avgBlkPerGame").put(season, new ArrayList<>());
                    statPerSeason.get("avgTovPerGame").put(season, new ArrayList<>());
                    statPerSeason.get("avgPtsPerGame").put(season, new ArrayList<>());
                }
                String abbreviation = values[1];
                Double playoffs = values[2].equals("TRUE") ? 1.0 : 0.0;
                int wins = Integer.parseInt(values[3]);
                int losses = Integer.parseInt(values[4]);
                double avgFgPercent = Double.parseDouble(values[5]);
                statPerSeason.get("avgFgPercent").get(season).add(avgFgPercent);
                double avgX3pPercent = Double.parseDouble(values[6]);
                statPerSeason.get("avgX3pPercent").get(season).add(avgX3pPercent);
                double avgFtPercent = Double.parseDouble(values[7]);
                statPerSeason.get("avgFtPercent").get(season).add(avgFtPercent);
                double avgTrbPerGame = Double.parseDouble(values[8]);
                statPerSeason.get("avgTrbPerGame").get(season).add(avgTrbPerGame);
                double avgAstPerGame = Double.parseDouble(values[9]);
                statPerSeason.get("avgAstPerGame").get(season).add(avgAstPerGame);
                double avgStlPerGame = Double.parseDouble(values[10]);
                statPerSeason.get("avgStlPerGame").get(season).add(avgStlPerGame);
                double avgBlkPerGame = Double.parseDouble(values[11]);
                statPerSeason.get("avgBlkPerGame").get(season).add(avgBlkPerGame);
                double avgTovPerGame = Double.parseDouble(values[12]);
                statPerSeason.get("avgTovPerGame").get(season).add(avgTovPerGame);
                double avgPtsPerGame = Double.parseDouble(values[13]);
                statPerSeason.get("avgPtsPerGame").get(season).add(avgPtsPerGame);

                Team newTeam = new Team(season, abbreviation, playoffs, wins, losses, avgFgPercent, avgX3pPercent, avgFtPercent, avgTrbPerGame, avgAstPerGame, avgStlPerGame, avgBlkPerGame, avgTovPerGame, avgPtsPerGame);
                // If the season key doesn't exist, add it
                data.put(counter, newTeam);
                counter++;
                // Add the team to the season

            }
            scanner.close();
            normalizeAll(); // normalize the data
            standardizeAll();

        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + filename);
            e.printStackTrace();
        }
    }


    public static double calculateStdDev(ArrayList<Double> values) {
        int n = values.size();
        if (n < 2) {
            throw new IllegalArgumentException("List must contain at least two values.");
        }

        // Calculate the mean of the values
        double sum = 0.0;
        for (double val : values) {
            sum += val;
        }
        double mean = sum / n;

        // Calculate the sum of the squared differences from the mean
        double squaredDiffsSum = 0.0;
        for (double val : values) {
            double diff = val - mean;
            squaredDiffsSum += diff * diff;
        }
        double variance = squaredDiffsSum / (n - 1);
        double stdDev = Math.sqrt(variance);

        return stdDev;
    }

    public static HashMap<Integer, Team> createTrainingData(){
        HashMap<Integer, Team> testingData = new HashMap<>();
        Random r = new Random();
        r.setSeed(1257);
        while(testingData.size() < data.size() * 0.8){
            int idx = r.nextInt(data.size());
            testingData.put(idx, data.get(idx));
        }
        return testingData;
    }

    public static HashMap<Integer, Team> createTestingData(){
        HashMap<Integer, Team> testingData = new HashMap<>();
        for(int i = 0; i < data.size(); i++){
            if(!trainingData.containsKey(i)){
                testingData.put(i, data.get(i));
            }
        }
        return testingData;
    }

    public static HashMap<Integer, HashMap<Integer, Team>> createCrossValidationSets(){
        HashMap<Integer, HashMap<Integer, Team>> crossValidation = new HashMap<>(); //key: which chunk of the data, NOT data point id
        Random r = new Random();
        r.setSeed(1257);
        for (int i = 0; i < 10; i++){
            HashMap<Integer, Team> chunk = new HashMap<>();
            for(int j = 0; j < trainingData.size() * 0.1; j++){
                int idx = r.nextInt(data.size());
                chunk.put(idx, trainingData.get(idx));
            }
            crossValidation.put(i, chunk);
        }
        return crossValidation;
    }

    public static HashMap<Integer, Double> performCrossValidation(){
        HashMap<Integer, Double> errors = new HashMap<>(); //integer = k, double = average error
        double singleChunkError;
        double avgChunkError;
        double allChunkErrors;
        for (int k = 1; k <= 30; k++){
            allChunkErrors = 0;
            for (Map.Entry<Integer, HashMap<Integer, Team>> chunk : crossValidationSets.entrySet()){
                singleChunkError = 0;
                for (Team team : chunk.getValue().values()){
                    Collection<Team> closestTeams = findKClosestTeams(chunk.getKey(), team, k);
                    double predictedNumWins = classifyTeam(closestTeams);
                    singleChunkError += Math.abs(predictedNumWins - team.getWins());
                }
                avgChunkError = singleChunkError / chunk.getValue().values().size();
                allChunkErrors += avgChunkError;

            }
            errors.put(k, allChunkErrors / 10);
            //average error of all chunks and add to Hashmap
        }
        return errors;
    }

    public static Collection<Team> findKClosestTeams(int chunkIndex, Team t, int k){
        HashMap<Double, Team> closestTeams = new HashMap<>(); //double = distance to t, team = otherteam to t
        HashMap<Integer, HashMap<Integer, Team>> crossValidationTraining = (HashMap<Integer, HashMap<Integer, Team>>) crossValidationSets.clone();
        if(chunkIndex != -1){
            crossValidationTraining.remove(chunkIndex);
        }
        //HashMap<Integer, Team> currentChunk = crossValidationSets.get(chunkIndex);
        for(HashMap<Integer, Team> chunks : crossValidationTraining.values()){
            for(Team otherTeam : chunks.values()){
                double distance = t.calculateL1Distance(otherTeam);
                if(closestTeams.size() < k){
                    closestTeams.put(distance, otherTeam);
                } else{
                    double maximumDistance = Collections.max(closestTeams.keySet());
                    if (distance < maximumDistance){
                        closestTeams.remove(maximumDistance);
                        closestTeams.put(distance, otherTeam);
                    }
                }
            }
        }

        return closestTeams.values();
    }

    public static double classifyTeam(Collection<Team> closestTeams){
        double totalWins = 0.0;
        for(Team t : closestTeams){
            totalWins += t.getWins();
        }
        return totalWins / closestTeams.size();
    }

    public static double calculateTestingError(int k){
        double totalError = 0.0;
        double avgError;
        double predictedWins;

        for(Team team : testingData.values()){
            Collection<Team> closestTeams = findKClosestTeams(-1, team, k);
            predictedWins = classifyTeam(closestTeams);
            totalError += Math.abs(team.getWins() - predictedWins);
        }

        avgError = totalError / testingData.values().size();

        return avgError;
    }


    public static void normalize(HashMap<String, HashMap<Integer, ArrayList<Double>>> nameLater, Team Team){
        double meanavgFgPercent = calculateMean(nameLater.get("avgFgPercent").get(Team.getSeason()));
        Team.setAvgFgPercent(Team.getAvgFgPercent()/meanavgFgPercent);
        TTavgFgPercent.add(Team.getAvgFgPercent());
        double meanavgX3pPercent = calculateMean(nameLater.get("avgFgPercent").get(Team.getSeason()));
        Team.setAvgX3pPercent(Team.getAvgX3pPercent()/meanavgX3pPercent);
        TTavgX3pPercent.add(Team.getAvgX3pPercent());
        Team.setAvgFtPercent(Team.getAvgFtPercent()/calculateMean(nameLater.get("avgFtPercent").get(Team.getSeason())));
        TTavgFtPercent.add(Team.getAvgFtPercent());
        Team.setAvgTrbPerGame(Team.getAvgTrbPerGame()/calculateMean(nameLater.get("avgTrbPerGame").get(Team.getSeason())));
        TTavgTrbPerGame.add(Team.getAvgTrbPerGame());
        Team.setAvgAstPerGame(Team.getAvgAstPerGame()/calculateMean(nameLater.get("avgAstPerGame").get(Team.getSeason())));
        TTavgAstPerGame.add(Team.getAvgAstPerGame());
        Team.setAvgStlPerGame(Team.getAvgStlPerGame()/calculateMean(nameLater.get("avgStlPerGame").get(Team.getSeason())));
        TTavgStlPerGame.add(Team.getAvgStlPerGame());
        Team.setAvgBlkPerGame(Team.getAvgBlkPerGame()/calculateMean(nameLater.get("avgBlkPerGame").get(Team.getSeason())));
        TTavgBlkPerGame.add(Team.getAvgBlkPerGame());
        Team.setAvgTovPerGame(Team.getAvgTovPerGame()/calculateMean(nameLater.get("avgTovPerGame").get(Team.getSeason())));
        TTavgTovPerGame.add(Team.getAvgTovPerGame());
        Team.setAvgPtsPerGame(Team.getAvgPtsPerGame()/calculateMean(nameLater.get("avgPtsPerGame").get(Team.getSeason())));
        TTavgPtsPerGame.add(Team.getAvgPtsPerGame());
    }
    public static void normalizeAll(){
        for(Map.Entry<Integer, Team> t : data.entrySet()){
            normalize(statPerSeason, t.getValue());

        }
    }

    public static double calculateMean(ArrayList<Double> list) {
        double sum = 0.0;
        for (double num : list) {
            sum += num;
        }
        return sum / list.size();
    }


    public static void standardizeAll(){
        for(Map.Entry<Integer, Team> t : data.entrySet()){
            t.getValue().standardize("avgFgPercent", calculateMean(TTavgFgPercent), calculateStdDev(TTavgFgPercent));
            t.getValue().standardize("avgX3pPercent", calculateMean(TTavgX3pPercent), calculateStdDev(TTavgX3pPercent));
            t.getValue().standardize("avgFtPercent", calculateMean(TTavgFtPercent), calculateStdDev(TTavgFtPercent));
            t.getValue().standardize("avgTrbPerGame", calculateMean(TTavgTrbPerGame), calculateStdDev(TTavgTrbPerGame));
            t.getValue().standardize("avgAstPerGame", calculateMean(TTavgAstPerGame), calculateStdDev(TTavgAstPerGame));
            t.getValue().standardize("avgStlPerGame", calculateMean(TTavgStlPerGame), calculateStdDev(TTavgStlPerGame));
            t.getValue().standardize("avgBlkPerGame", calculateMean(TTavgBlkPerGame), calculateStdDev(TTavgBlkPerGame));
            t.getValue().standardize("avgTovPerGame", calculateMean(TTavgTovPerGame), calculateStdDev(TTavgTovPerGame));
            t.getValue().standardize("avgPtsPerGame", calculateMean(TTavgPtsPerGame), calculateStdDev(TTavgPtsPerGame));
        }
    }

    public static void calculate2023Wins(){
        //if you want to get win totals you should uncomment the print statement in the classifyTeam function
        process("Final_Project/2023data.csv");
        for(int i = 0; i < 30; i++){
            testingData.put(i, data.get(i));
        }
        for(int i = 30; i < 1257; i++){
            trainingData.put(i, data.get(i));
        }
        //this essentially puts the 2023 season as the testing data and everything else as the training data in one chunk
        crossValidationSets.put(1, (HashMap<Integer, Team>) trainingData.clone());

        System.out.println(calculateTestingError(2));

    }
}
