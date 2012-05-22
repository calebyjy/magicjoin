package joins;

import java.util.Random;  

public class BPlusTree  {  
      
    /** ���ڵ� */  
    protected Node root;  
      
    /** ������Mֵ */  
    protected int order;  
      
    /** Ҷ�ӽڵ������ͷ*/  
    protected Node head;  
      
    public Node getHead() {  
        return head;  
    }  
  
    public void setHead(Node head) {  
        this.head = head;  
    }  
  
    public Node getRoot() {  
        return root;  
    }  
  
    public void setRoot(Node root) {  
        this.root = root;  
    }  
  
    public int getOrder() {  
        return order;  
    }  
  
    public void setOrder(int order) {  
        this.order = order;  
    }  
  
    public Object get(Comparable key) {  
        return root.get(key);  
    }  
  
    public void remove(Comparable key) {  
        root.remove(key, this);  
  
    }  
  
    public void insertOrUpdate(Comparable key, Object obj) {  
        root.insertOrUpdate(key, obj, this);  
  
    }  
      
    public BPlusTree(int order){  
        if (order < 3) {  
            System.out.print("order must be greater than 2");  
            System.exit(0);  
        }  
        this.order = order;  
        root = new Node(true, true);  
        head = root;  
    }  
      
    //����  
    public static void main(String[] args) {  
        BPlusTree tree = new BPlusTree(6);  
        Random random = new Random();  
        long current = System.currentTimeMillis();  
        for (int j = 0; j < 100000; j++) {  
            for (int i = 0; i < 100; i++) {  
                int randomNumber = random.nextInt(1000);  
                tree.insertOrUpdate(randomNumber, randomNumber);  
            }  
  
            for (int i = 0; i < 100; i++) {  
                int randomNumber = random.nextInt(1000);  
                tree.remove(randomNumber);  
            }  
        }  
  
        long duration = System.currentTimeMillis() - current;  
        System.out.println("time elpsed for duration: " + duration);  
        int search = 80;  
        System.out.print(tree.get(search));  
    }  
  
}