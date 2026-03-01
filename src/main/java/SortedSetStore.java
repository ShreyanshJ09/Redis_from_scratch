import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SortedSetStore {
    private final Map<String, SortedSet> sortedSets = new ConcurrentHashMap<>();
    
    public synchronized int zadd(String key, double score, String member) {
        SortedSet zset = sortedSets.computeIfAbsent(key, k -> new SortedSet());
        return zset.add(member, score);
    }
    
    public synchronized Integer zrank(String key, String member) {
        SortedSet zset = sortedSets.get(key);
        if (zset == null) return null;
        return zset.getRank(member);
    }
    

    public synchronized List<String> zrange(String key, int start, int stop) {
        SortedSet zset = sortedSets.get(key);
        if (zset == null) return Collections.emptyList();
        return zset.getRange(start, stop);
    }
    
    public boolean exists(String key) {
        return sortedSets.containsKey(key);
    }
    
    public synchronized int zcard(String key) {
        SortedSet zset = sortedSets.get(key);
        return zset != null ? zset.size() : 0;
    }
    
    public List<String> getAllKeys() {
        return new ArrayList<>(sortedSets.keySet());
    }
}

class SortedSet {
    private final Map<String, Double> memberToScore = new HashMap<>();
    
    private final TreeMap<ScoredMember, String> sortedMembers = new TreeMap<>();
    

    public int add(String member, double score) {
        Double oldScore = memberToScore.get(member);
        
        if (oldScore != null) {
            sortedMembers.remove(new ScoredMember(oldScore, member));
            
            memberToScore.put(member, score);
            sortedMembers.put(new ScoredMember(score, member), member);
            
            return 0;
        } else {
            memberToScore.put(member, score);
            sortedMembers.put(new ScoredMember(score, member), member);
            
            return 1;
        }
    }
    

    public Integer getRank(String member) {
        Double score = memberToScore.get(member);
        if (score == null) return null;
        
        ScoredMember key = new ScoredMember(score, member);
        
        return sortedMembers.headMap(key, false).size();
    }
    
    public List<String> getRange(int start, int stop) {
        int size = sortedMembers.size();
        
        if (size == 0) {
            return Collections.emptyList();
        }
        
        if (start < 0) {
            start = size + start;
            if (start < 0) start = 0;
        }
        
        if (stop < 0) {
            stop = size + stop;
            if (stop < 0) stop = 0;
        }
        
        if (stop >= size) {
            stop = size - 1;
        }
        
        if (start > stop || start >= size) {
            return Collections.emptyList();
        }
        
        List<String> result = new ArrayList<>();
        int index = 0;
        
        for (String member : sortedMembers.values()) {
            if (index >= start && index <= stop) {
                result.add(member);
            }
            if (index > stop) {
                break;
            }
            index++;
        }
        
        return result;
    }
    
    public int size() {
        return memberToScore.size();
    }
}


class ScoredMember implements Comparable<ScoredMember> {
    final double score;
    final String member;
    
    public ScoredMember(double score, String member) {
        this.score = score;
        this.member = member;
    }
    
    @Override
    public int compareTo(ScoredMember other) {
        int scoreCompare = Double.compare(this.score, other.score);
        if (scoreCompare != 0) {
            return scoreCompare;
        }
        
        return this.member.compareTo(other.member);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ScoredMember)) return false;
        ScoredMember other = (ScoredMember) obj;
        return Double.compare(score, other.score) == 0 && 
               member.equals(other.member);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(score, member);
    }
}