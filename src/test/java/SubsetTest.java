import com.google.common.base.Joiner;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class SubsetTest {
    public static void main(String sss[]){
        long st=System.currentTimeMillis();
        Map<Integer, String> sortMap = new TreeMap<Integer, String>();


//        String[] es="1,3".split(",");
        String[] es="1==1,2==3".split(",");
        for (String tmp:es) {
            String args[]=tmp.split("==");
            sortMap.put(Integer.parseInt(args[0]),args[1]);
        }

        es=sortMap.values().toArray(es);
        String[] fs ="1,3".split(",");
        String[] match=new String[fs.length];
        Arrays.fill(match, "0");
        int idx=0;
        for (int i=0;i<fs.length;i++){
            for (int j=idx;j<es.length;j++){
                if(fs[i].equalsIgnoreCase(es[j])){
                    idx=j+1;
                    match[i]="1";
                    break;
                }
            }
        }

        String str= Joiner.on(",").join(match);
        System.out.println((System.currentTimeMillis()-st));
        System.out.println(str);
    }
}
