

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SimilarityMapper extends
		Mapper<Object, Text, Text, Text> {

	private Text words = new Text();
	private Text filename = new Text();
	public static String ngram = "";
	public static long wordsCount = 0;
	public static int numWords=0;
	public static int nGramValue = 3;
	public static String[] STOPWORDS = {
			"without", "see", "unless", "due", "also", "must", "might", "like", "]", "[", "}", "{", "<", ">", "?", "\"", "\\", "/", ")", "(", "will", "may", "can", "much", "every", "the", "in", "other", "this", 
			"the", "many", "any", "an", "or", "for", "in", "an", "an ", "is", "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren’t", "as", "at", "be", "because", "been", 
			"before", "being", "below", "between", "both", "but", "by", "can’t", "cannot", "could",
			"couldn’t", "did", "didn’t", "do", "does", "doesn’t", "doing", "don’t", "down", "during", "each", "few", "for", "from", "further", "had", "hadn’t", "has", "hasn’t", "have", "haven’t", "having",
			"he", "he’d", "he’ll", "he’s", "her", "here", "here’s", "hers", "herself", "him", "himself", "his", "how", "how’s", "i ", " i", "i’d", "i’ll", "i’m", "i’ve", "if", "in", "into", "is",
			"isn’t", "it", "it’s", "its", "itself", "let’s", "me", "more", "most", "mustn’t", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "ought", "our", "ours", "ourselves",
			"out", "over", "own", "same", "shan’t", "she", "she’d", "she’ll", "she’s", "should", "shouldn’t", "so", "some", "such", "than",
			"that", "that’s", "their", "theirs", "them", "themselves", "then", "there", "there’s", "these", "they", "they’d", "they’ll", "they’re", "they’ve",
			"this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn’t", "we", "we’d", "we’ll", "we’re", "we’ve",
			"were", "weren’t", "what", "what’s", "when", "when’s", "where", "where’s", "which", "while", "who", "who’s", "whom",
			"why", "why’s", "with", "won’t", "would", "wouldn’t", "you", "you’d", "you’ll", "you’re", "you’ve", "your", "yours", "yourself", "yourselves",
			"Without", "See", "Unless", "Due", "Also", "Must", "Might", "Like", "Will", "May", "Can", "Much", "Every", "The", "In", "Other", "This", "The", "Many", "Any", "An", "Or", "For", "In", "An", "An ", "Is", "A", 
			"About", "Above", "After", "Again", "Against", "All", "Am", "An", "And", "Any", "Are", "Aren’t", "As", "At", "Be", "Because", "Been", "Before", "Being", "Below", "Between", "Both", "But", "By", "Can’t", "Cannot", "Could",
			"Couldn’t", "Did", "Didn’t", "Do", "Does", "Doesn’t", "Doing", "Don’t", "Down", "During", "Each", "Few", "For", "From", "Further", "Had", "Hadn’t", "Has", "Hasn’t", "Have", "Haven’t", "Having",
			"He", "He’d", "He’ll", "He’s", "Her", "Here", "Here’s", "Hers", "Herself", "Him", "Himself", "His", "How", "How’s", "I ", " I", "I’d", "I’ll", "I’m", "I’ve", "If", "In", "Into", "Is",
			"Isn’t", "It", "It’s", "Its", "Itself", "Let’s", "Me", "More", "Most", "Mustn’t", "My", "Myself", "No", "Nor", "Not", "Of", "Off", "On", "Once", "Only", "Ought", "Our", "Ours", "Ourselves",
			"Out", "Over", "Own", "Same", "Shan’t", "She", "She’d", "She’ll", "She’s", "Should", "Shouldn’t", "So", "Some", "Such", "Than",
			"That", "That’s", "Their", "Theirs", "Them", "Themselves", "Then", "There", "There’s", "These", "They", "They’d", "They’ll", "They’re", "They’ve",
			"This", "Those", "Through", "To", "Too", "Under", "Until", "Up", "Very", "Was", "Wasn’t", "We", "We’d", "We’ll", "We’re", "We’ve",
			"Were", "Weren’t", "What", "What’s", "When", "When’s", "Where", "Where’s", "Which", "While", "Who", "Who’s", "Whom",
			"Why", "Why’s", "With", "Won’t", "Would", "Wouldn’t", "You", "You’d", "You’ll", "You’re", "You’ve", "Your", "Yours", "Yourself", "Yourselves"
			};
	public static String ALLOWEDCHARACTERS="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
		filename = new Text(filenameStr);
		String line = value.toString();
		String word = "";
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word=tokenizer.nextToken(); 
			word=filterPunctuations(word); 
			word=filterStopWords(word); 
			if (word.length()!=0){ 
			word = getNGram(word,nGramValue);
			if (word.length()!= 0) {
			words.set(word);
			}	 
			}			
			context.write(words, filename);
		}
	}
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
	}
   
    /**
     * Creates n-gram of specified value.
     * @param word
     * @param nGramValue
     * @return
     */
	public static String getNGram(String word, int nGramValue){
		String result = "";
		if (wordsCount< nGramValue){
			ngram = ngram+" "+word;
			wordsCount++;
		}
		if (wordsCount == nGramValue){
			result = ngram.trim();
			wordsCount = nGramValue-1;
			ngram = result.substring(result.indexOf(" ",0)+1, result.length());
		}
		return result;
	}
	
	/**
	 * Filters stop words!
	 * @param word
	 * @return
	 */
	public static String filterStopWords(String word){
		for (int i=0; i<STOPWORDS.length; i++){
			if (word.equals(STOPWORDS[i])){
				word="";
			}
		}
		return word;
	}
	
	/**
	 * Filters punctuations!	
	 * @param word
	 * @return
	 */
	public static String filterPunctuations(String word){
		int len=0;
		char singleChar=0;
		while(true){
			word=word.trim();
			len=word.length();
		if (len==0) break;
		singleChar=word.charAt(0);
		if(ALLOWEDCHARACTERS.indexOf(Character.toString(singleChar))==-1){
			word=word.substring(1,len);
		}else{
			break;
		 }
		}
		while(true){
			word=word.trim();
			len=word.length();
		if(len==0) break;
		singleChar=word.charAt(len-1);
		if(ALLOWEDCHARACTERS.indexOf(Character.toString(singleChar))==-1){
			word=word.substring(0,len-1);
		}else{
			break;
		 }
		}
	return word.toLowerCase(); 
	}
}