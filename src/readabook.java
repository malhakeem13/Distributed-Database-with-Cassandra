import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.utils.Bytes;




public class readabook {
   
    public static Cluster cluster = null;
    public static Session session ;
    public static ResultSet Results;
    public static Row Rows;
    public static String keyspace = "readabook";
    public static InetAddress    node1;
    public static InetAddress node2;
    public static InetAddress node3;

   
    public static void main(String Args[]) throws IOException{
        boolean successLogin = false;
       
        System.out.println("***Welcome to Share A Book***");
        while( successLogin == false)
        {
            successLogin = Login();
            if (successLogin == false)
                System.out.println("Wrong credintial, please try again:");
        }
       
        Menu();
       
    }
   
    private static void Menu() throws IOException {
        System.out.println("Welcome to the files directory, please select an action:");
        System.out.println("1. View all books: ");
        System.out.println("2. Add a book:");
       
        Scanner userInput = new Scanner(System.in);
       
        int selection = userInput.nextInt();
        if (selection ==1)
            ViewAll();
        else if (selection == 2)
            Addabook();
        else
        {
            System.out.println("Wrong Entry!");
            Menu();
        }
       
    }

    private static void Addabook() {
        InputStream inStream = null;
       
          

        try{

            //insert details of the book:
            Scanner userInput = new Scanner(System.in);
           
            System.out.print("Enter Book title: ");
            String title = userInput.nextLine();
            System.out.print("Enter book Category: ");
            String category = userInput.nextLine();
            System.out.print("Enter year: ");
            String year = userInput.nextLine();
            System.out.print("Enter book ISBN: ");
            String isbn = userInput.nextLine();
            System.out.print("Enter Language: ");
            String language = userInput.nextLine();
            System.out.print("Enter Author:");
            String author = userInput.nextLine();
            System.out.print("Enter book file location: ");
            String file = userInput.nextLine();
           
            File afile =new File(file);
          
            inStream = new FileInputStream(afile);
           
            node1 = InetAddress.getByName("127.0.0.1");
            node2 = InetAddress.getByName("127.0.0.2");
            node3 = InetAddress.getByName("127.0.0.3");
           
            Collection<InetAddress> addresses = new ArrayList<InetAddress>();
            addresses.add(node1);
            addresses.add(node2);
            addresses.add(node3);
          
            byte[] buffer = new byte[800000];
            inStream.read(buffer);

            Cluster cluster = Cluster.builder().addContactPoints(addresses).
                    withRetryPolicy(DefaultRetryPolicy.INSTANCE).
                    withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy())).build();
          
            session = cluster.connect(keyspace);
           
           
            PreparedStatement ps =
                    session.prepare("insert into book (title, category,author,"
                    +"year, isbn, language, file) values (?,?,?,?,?,?,?)");
            BoundStatement boundStatement = new BoundStatement(ps);

            session.execute(boundStatement.bind(title,category,author,year,isbn,language,ByteBuffer.wrap(buffer)));

            System.out.println("Book is added successful!");
            Menu();

        }catch(IOException e){
            e.printStackTrace();
        }
       
    }

    public static void ViewAll() throws IOException {
        try {
            node1 = InetAddress.getByName("127.0.0.1");
            node2 = InetAddress.getByName("127.0.0.2");
            node3 = InetAddress.getByName("127.0.0.3");
           
            Collection<InetAddress> addresses = new ArrayList<InetAddress>();
            addresses.add(node1);
            addresses.add(node2);
            addresses.add(node3);
           
            cluster = Cluster.builder().addContactPoints(addresses).
                    withRetryPolicy(DefaultRetryPolicy.INSTANCE).
                    withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy())).build();
           
            session = cluster.connect(keyspace);
            String query = "select * from book";
            Results = session.execute(query);
            System.out.format("| %15s | %13s | %30s | %20s | %20s  \n", "title","isbn",
                    "category","language","year");
            for (Row row : Results)
            {
                System.out.format("| %15s | %13s | %30s | %20s | %20s  \n", row.getString("title"),row.getString("isbn"),
                        row.getString("category"), row.getString("language"),row.getString("year"));
            }}
        catch (UnknownHostException e){
            e.printStackTrace();
                    }
        finally {
            cluster.close();
           
        }
       
        System.out.println("Select an action:");
        System.out.println("1. Download a book:");
        System.out.println("2. Back to menu");
        System.out.println("3. Delete a book:");
        System.out.println("4. Filter books by category:");       
        Scanner userInput1 = new Scanner(System.in);
       
        int selection = userInput1.nextInt();
        
        if (selection ==4)
        {
        	
        	
        	try {
        		Scanner userInput = new Scanner(System.in);
        		System.out.println("Enter Book Category: ");
                
                String catEntered = userInput.nextLine();
        		
                node1 = InetAddress.getByName("127.0.0.1");
                node2 = InetAddress.getByName("127.0.0.2");
                node3 = InetAddress.getByName("127.0.0.3");
               
                Collection<InetAddress> addresses = new ArrayList<InetAddress>();
                addresses.add(node1);
                addresses.add(node2);
                addresses.add(node3);
               
                cluster = Cluster.builder().addContactPoints(addresses).
                        withRetryPolicy(DefaultRetryPolicy.INSTANCE).
                        withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy())).build();
               
                session = cluster.connect(keyspace);
                String query = "select * from book where category = '"+catEntered+"';";
                Results = session.execute(query);
                //System.out.format("| Book Name    | ID   |%n");
                System.out.format("| %15s | %13s | %30s | %20s | %20s  \n", "title","isbn",
                        "category","language","year");
                for (Row row : Results)
                {
                    System.out.format("| %15s | %13s | %30s | %20s | %20s  \n", row.getString("title"),row.getString("isbn"),
                            row.getString("category"), row.getString("language"),row.getString("year"));
                    //System.out.println(row.getList("author",String.class ));
                }
                Menu();}
            catch (UnknownHostException e){
                e.printStackTrace();
                        }
            finally {
                cluster.close();
               
            }
        	
        }
        
        else if (selection == 3)
            
        {
        System.out.println("enter Book isbn to delete: ");
       
        try {
            Scanner userInput = new Scanner(System.in);
           
            String isbnEntered = userInput.nextLine();
           
            node1 = InetAddress.getByName("127.0.0.1");
            node2 = InetAddress.getByName("127.0.0.2");
            node3 = InetAddress.getByName("127.0.0.3");
           
            Collection<InetAddress> addresses = new ArrayList<InetAddress>();
            addresses.add(node1);
            addresses.add(node2);
            addresses.add(node3);
           
            cluster = Cluster.builder().addContactPoints(addresses).
                    withRetryPolicy(DefaultRetryPolicy.INSTANCE).
                    withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy())).build();
           
            session = cluster.connect(keyspace);
            String query = "delete from book where isbn = '"+isbnEntered+"'allow filtering;";
            Results = session.execute(query);
            System.out.println("Book Deleted");
            Menu();
           }
        catch (UnknownHostException e){
            e.printStackTrace();
                    }
        finally {
            cluster.close();
           
        }
        }
        
        else if (selection ==2)
            Menu();
        else if (selection == 1)
       
        {
        //download a book
        System.out.println("enter Book isbn to download: ");
       
        try {
            Scanner userInput = new Scanner(System.in);
           
            
            String isbnEntered = userInput.nextLine();
           
            node1 = InetAddress.getByName("127.0.0.1");
            node2 = InetAddress.getByName("127.0.0.2");
            node3 = InetAddress.getByName("127.0.0.3");
           
            Collection<InetAddress> addresses = new ArrayList<InetAddress>();
            addresses.add(node1);
            addresses.add(node2);
            addresses.add(node3);
           
            cluster = Cluster.builder().addContactPoints(addresses).
                    withRetryPolicy(DefaultRetryPolicy.INSTANCE).
                    withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy())).build();
           
            session = cluster.connect(keyspace);
            String query = "select file from book where isbn = '"+isbnEntered+"' allow filtering;";
            Results = session.execute(query);
            Row row = Results.one();
            byte[] data = Bytes.getArray(row.getBytes("file"));
           
            String location = "/home/moh/Desktop/Downloads/"+isbnEntered+".pdf";
            File f = new File(location);
            f.getParentFile().mkdirs();
            f.createNewFile();
            OutputStream out = new FileOutputStream(location);
           
            out.write(data);
            System.out.println("Check your desktop Downloads");
            Menu();
           
            }
        catch (UnknownHostException e){
            e.printStackTrace();
                    }
        finally {
            cluster.close();
           
        }
        }
        else
        {
            System.out.println("Wrong Entry!");
            ViewAll();
        }
       
       
           
           
       
       
    }

    public static boolean Login(){
    	
       
        try {
            node1 = InetAddress.getByName("127.0.0.1");
            node2 = InetAddress.getByName("127.0.0.2");
            node3 = InetAddress.getByName("127.0.0.3");
           
            Collection<InetAddress> addresses = new ArrayList<InetAddress>();
            addresses.add(node1);
            addresses.add(node2);
            addresses.add(node3);
           
            cluster = Cluster.builder().addContactPoints(addresses).
                    withRetryPolicy(DefaultRetryPolicy.INSTANCE).
                    withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy())).build();
           
            session = cluster.connect(keyspace);
           
            Scanner userInput = new Scanner(System.in);
           
            System.out.print("Enter Username: ");
            String username = userInput.nextLine();
            System.out.print("Enter Password: ");
            String password = userInput.nextLine();
           
            String query = "select * from users where username = '"+ username +
                    "' and password = '" + password +"'";
            Results = session.execute(query);
            Rows = Results.one();
            if (Rows != null)
            {
                return  true;
            }
            else{
                return false;
            }
        }
           
            catch (UnknownHostException e){
                e.printStackTrace();
                return false;
            }
            finally {
                cluster.close();
               
            }
        }
            }