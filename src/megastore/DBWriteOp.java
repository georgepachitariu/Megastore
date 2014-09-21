package megastore;

import java.util.List;

/**
 * Created by George on 29/06/2014.
 */
public class DBWriteOp implements Runnable {

    private List<Write> writes;
    private Entity entity;
    private Boolean answer;
    private Boolean isWeak;

    public DBWriteOp(Entity e, List<Write> writes, Boolean isWeak) {
        this.entity=e;
        this.writes=writes;
        this.isWeak=isWeak;
        answer=null;
    }

    public boolean execute() {
        new Thread(this).start();

        while(answer==null) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return answer;
    }

    public synchronized void setAnswer(Boolean answer) {
        if(this.answer==null)
            this.answer = answer;
    }

    @Override
    public void run() {
        entity.put(writes, this, isWeak);
    }
}
