package systemlog;

import java.util.LinkedList;

/**
 * Created by George on 17/06/2014.
 */
public class LogAnalyzer {
    private LinkedList<LogCell> log;
    public LinkedList<LogCell>[] splittedLog;

    public LogAnalyzer() {
        this.log=SystemLog.log;
    }

    public void printNrOfSuccessesAndFailuresPerSecond() {
        int successes = 0;
        int failures = 0;

        int[] sVector=new int[100];
        int[] fVector=new int[100];
        long reference=-1;
        int size=0;

        for (LogCell cell : log) {
            if (cell instanceof OperationLogCell) {
                OperationLogCell nCell = (OperationLogCell) cell;
                if(reference==-1)
                    reference=nCell.timestamp/1000-1;
                if(size<(int)(nCell.timestamp/1000-reference))
                    size=(int)(nCell.timestamp/1000-reference);

                if (nCell.succeeded)
                    sVector[(int)(nCell.timestamp/1000-reference)]++;
                else
                    fVector[(int)(nCell.timestamp/1000-reference)]++;
            }
        }


        successes = 0;
        for (int i = 3; i < size-1; i++)
            successes += sVector[i];
        System.out.println("Successes: " + (double) successes / (size-4));

        failures = 0;
        for (int i = 3; i < size-1; i++)
            failures += fVector[i];
        System.out.println("Failures: " + (double) failures / (size-4));
    }

    public void divideLog() {
        LinkedList<String> urls=new LinkedList<String>();

        for(int i=0; i<20 && log.size()>i; i++)
            if(log.get(i) instanceof OperationLogCell) {
                OperationLogCell cell=(OperationLogCell)log.get(i);
                if(! urls.contains(cell.nodeUrl))
                    urls.add(cell.nodeUrl);
            }

        splittedLog=new LinkedList[urls.size()];
        for(int i=0; i<urls.size(); i++)
            splittedLog[i]=new LinkedList<LogCell>();

        for(LogCell cell : log)
            splittedLog[urls.indexOf(cell.getNode())].add(cell);
    }

    public void countTheMethodsUsedForEachWrite() {
        int twoPhaseSuccess=0;
        int twoPhaseFailed=0;
        int onePhaseSuccess=0;
        int onePhaseFailed=0;

        for(int i=0; i<splittedLog.length; i++) {
            for(int j=0; j<splittedLog[i].size(); j=j+2) {
                SystemLogCell cell=(SystemLogCell)splittedLog[i].get(j);
                OperationLogCell opCell=(OperationLogCell) splittedLog[i].get(j+1);

                if(cell.message.equals("Two Rounds")) {
                    if (opCell.succeeded)
                        twoPhaseSuccess++;
                    else
                        twoPhaseFailed++;
                }
                else
                if(cell.message.equals("One Round")) {
                    if (opCell.succeeded)
                        onePhaseSuccess++;
                    else
                        onePhaseFailed++;
                }
            }
        }
        System.out.println("Two Phase Paxos Succeeded: "+ twoPhaseSuccess);
        System.out.println("Two Phase Paxos Failed: "+ twoPhaseFailed);
        System.out.println("One Phase Paxos Succeeded: "+ onePhaseSuccess);
        System.out.println("One Phase Paxos Failed: "+ onePhaseFailed);
    }




    public void showMedianPerfomanceTimes() {
        LinkedList<Integer> twoPhaseSuccess=new LinkedList<Integer>();
        LinkedList<Integer> twoPhaseFailed=new LinkedList<Integer>();
        LinkedList<Integer> onePhaseSuccess=new LinkedList<Integer>();
        LinkedList<Integer> onePhaseFailed=new LinkedList<Integer>();

        for(int i=0; i<splittedLog.length; i++) {
            for(int j=0; j<splittedLog[i].size(); j=j+2) {
                SystemLogCell cell=(SystemLogCell)splittedLog[i].get(j);
                OperationLogCell nextCell=(OperationLogCell) splittedLog[i].get(j+1);

                if(cell.message.equals("Two Rounds")) {
                    if (nextCell.succeeded)
                        twoPhaseSuccess.add((int)nextCell.duration);
                    else
                        twoPhaseFailed.add((int)nextCell.duration);
                }
                else
                if(cell.message.equals("One Round")) {
                    if (nextCell.succeeded) {
                        onePhaseSuccess.add((int) nextCell.duration);
         //               System.out.println(nextCell.duration);
                    }
                    else
                        onePhaseFailed.add((int)nextCell.duration);
                }
            }
        }


  //      System.out.println("Two Phase Paxos Succeeded Avg Time: "+ getAvg(twoPhaseSuccess));
  //      System.out.println("Two Phase Paxos Failed Avg Time: "+ getAvg(twoPhaseFailed));
        System.out.println("One Phase Paxos Succeeded Avg Time: "+ getAvg(onePhaseSuccess));
 //       System.out.println("One Phase Paxos Failed Avg Time: "+ getAvg(onePhaseFailed));
    }


    private float getAvg(LinkedList<Integer> list) {
        int sum=0;
        for(int x : list)
            sum+=x;
        return (float)sum/list.size();
    }

}
