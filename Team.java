import java.util.ArrayList;
import java.util.HashMap;


public class Team {
    private Integer season;
    private String abbreviation;
    private double playoffs;
    private Integer wins;
    private Integer losses;
    private double avgFgPercent;
    private double avgX3pPercent;
    private double avgFtPercent;
    private double avgTrbPerGame;
    private double avgAstPerGame;
    private double avgStlPerGame;
    private double avgBlkPerGame;
    private double avgTovPerGame;
    private double avgPtsPerGame;



    // constructor
    public Team(Integer season, String abbreviation, double playoffs, Integer wins, Integer losses, double avgFgPercent, double avgX3pPercent, double avgFtPercent, double avgTrbPerGame, double avgAstPerGame, double avgStlPerGame, double avgBlkPerGame, double avgTovPerGame, double avgPtsPerGame) {
        this.season = season;
        this.abbreviation = abbreviation;
        this.playoffs = playoffs;
        this.wins = wins;
        this.losses = losses;
        this.avgFgPercent = avgFgPercent;
        this.avgX3pPercent = avgX3pPercent;
        this.avgFtPercent = avgFtPercent;
        this.avgTrbPerGame = avgTrbPerGame;
        this.avgAstPerGame = avgAstPerGame;
        this.avgStlPerGame = avgStlPerGame;
        this.avgBlkPerGame = avgBlkPerGame;
        this.avgTovPerGame = avgTovPerGame;
        this.avgPtsPerGame = avgPtsPerGame;
    }

    public double calculateL2Distance(Team otherTeam) {
        double distance = Math.sqrt(
                Math.pow(this.avgFgPercent - otherTeam.getAvgFgPercent(), 2) +
                        Math.pow(this.avgX3pPercent - otherTeam.getAvgX3pPercent(), 2) +
                        Math.pow(this.avgFtPercent - otherTeam.getAvgFtPercent(), 2) +
                        Math.pow(this.avgTrbPerGame - otherTeam.getAvgTrbPerGame(), 2) +
                        Math.pow(this.avgAstPerGame - otherTeam.getAvgAstPerGame(), 2) +
                        Math.pow(this.avgStlPerGame - otherTeam.getAvgStlPerGame(), 2) +
                        Math.pow(this.avgBlkPerGame - otherTeam.getAvgBlkPerGame(), 2) +
                        Math.pow(this.avgTovPerGame - otherTeam.getAvgTovPerGame(), 2) +
                        Math.pow(this.avgPtsPerGame - otherTeam.getAvgPtsPerGame(), 2)
        );
        return distance;
    }

    public double calculateL1Distance(Team otherTeam) {
        double distance =
                Math.abs(this.avgFgPercent - otherTeam.getAvgFgPercent()) +
                        Math.abs(this.avgX3pPercent - otherTeam.getAvgX3pPercent()) +
                        Math.abs(this.avgFtPercent - otherTeam.getAvgFtPercent()) +
                        Math.abs(this.avgTrbPerGame - otherTeam.getAvgTrbPerGame()) +
                        Math.abs(this.avgAstPerGame - otherTeam.getAvgAstPerGame()) +
                        Math.abs(this.avgStlPerGame - otherTeam.getAvgStlPerGame()) +
                        Math.abs(this.avgBlkPerGame - otherTeam.getAvgBlkPerGame()) +
                        Math.abs(this.avgTovPerGame - otherTeam.getAvgTovPerGame()) +
                        Math.abs(this.avgPtsPerGame - otherTeam.getAvgPtsPerGame()
                        );
        return distance;
    }




    public void standardize(String attribute, double mean, double std){
        if(attribute.equals("avgFgPercent")){
            this.avgFgPercent = (this.avgFtPercent - mean) / std;
        }
        if(attribute.equals("avgX3pPercent")){
            this.avgX3pPercent = (this.avgX3pPercent - mean) / std;
        }
        if(attribute.equals("avgFtPercent")){
            this.avgFtPercent = (this.avgFtPercent - mean) / std;
        }
        if(attribute.equals("avgTrbPerGame")){
            this.avgTrbPerGame = (this.avgTrbPerGame - mean) / std;
        }
        if(attribute.equals("avgAstPerGame")){
            this.avgAstPerGame = (this.avgAstPerGame - mean) / std;
        }
        if(attribute.equals("avgStlPerGame")){
            this.avgStlPerGame = (this.avgStlPerGame - mean) / std;
        }
        if(attribute.equals("avgBlkPerGame")){
            this.avgBlkPerGame = (this.avgBlkPerGame - mean) / std;
        }
        if(attribute.equals("avgTovPerGame")){
            this.avgTovPerGame = (this.avgTovPerGame - mean) / std;
        }
        if(attribute.equals("avgPtsPerGame")){
            this.avgPtsPerGame = (this.avgPtsPerGame - mean) / std;
        }
    }


    // getters and setters for all attributes

    @Override
    public String toString() {
        return "team{" +
                "season='" + season + '\'' +
                ", abbreviation='" + abbreviation + '\'' +
                ", playoffs=" + playoffs +
                ", wins=" + wins +
                ", losses=" + losses +
                ", avgFgPercent=" + avgFgPercent +
                ", avgX3pPercent=" + avgX3pPercent +
                ", avgFtPercent=" + avgFtPercent +
                ", avgTrbPerGame=" + avgTrbPerGame +
                ", avgAstPerGame=" + avgAstPerGame +
                ", avgStlPerGame=" + avgStlPerGame +
                ", avgBlkPerGame=" + avgBlkPerGame +
                ", avgTovPerGame=" + avgTovPerGame +
                ", avgPtsPerGame=" + avgPtsPerGame +
                '}';
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    public double getPlayoffs() {
        return playoffs;
    }

    public Integer getSeason() {
        return season;
    }

    public Integer getWins() {
        return wins;
    }

    public Integer getLosses() {
        return losses;
    }

    public double getAvgFgPercent() {
        return avgFgPercent;
    }

    public double getAvgX3pPercent() {
        return avgX3pPercent;
    }

    public double getAvgFtPercent() {
        return avgFtPercent;
    }

    public double getAvgTrbPerGame() {
        return avgTrbPerGame;
    }

    public double getAvgAstPerGame() {
        return avgAstPerGame;
    }

    public double getAvgStlPerGame() {
        return avgStlPerGame;
    }

    public double getAvgBlkPerGame() {
        return avgBlkPerGame;
    }

    public double getAvgTovPerGame() {
        return avgTovPerGame;
    }

    public double getAvgPtsPerGame() {
        return avgPtsPerGame;
    }

    public void setWins(Integer wins) {
        this.wins = wins;
    }

    public void setLosses(Integer losses) {
        this.losses = losses;
    }

    public void setAvgFgPercent(double avgFgPercent) {
        this.avgFgPercent = avgFgPercent;
    }

    public void setAvgX3pPercent(double avgX3pPercent) {
        this.avgX3pPercent = avgX3pPercent;
    }

    public void setAvgFtPercent(double avgFtPercent) {
        this.avgFtPercent = avgFtPercent;
    }

    public void setAvgTrbPerGame(double avgTrbPerGame) {
        this.avgTrbPerGame = avgTrbPerGame;
    }

    public void setAvgAstPerGame(double avgAstPerGame) {
        this.avgAstPerGame = avgAstPerGame;
    }

    public void setAvgStlPerGame(double avgStlPerGame) {
        this.avgStlPerGame = avgStlPerGame;
    }

    public void setAvgBlkPerGame(double avgBlkPerGame) {
        this.avgBlkPerGame = avgBlkPerGame;
    }

    public void setAvgTovPerGame(double avgTovPerGame) {
        this.avgTovPerGame = avgTovPerGame;
    }

    public void setAvgPtsPerGame(double avgPtsPerGame) {
        this.avgPtsPerGame = avgPtsPerGame;
    }
}