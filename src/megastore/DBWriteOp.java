package megastore;

/**
 * Created by George on 29/06/2014.
 */
public class DBWriteOp implements Runnable {

    private String value;
    private String key;
    private Entity entity;
    private Boolean answer;

    public DBWriteOp(Entity e, String key, String value) {
        this.entity=e;
        this.key=key;
        this.value=value;
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
        entity.put(key,value, this);
    }
}
