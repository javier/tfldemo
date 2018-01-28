package com.teowaki.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class StringUtil {


    /**
     * Parse a line of text in CSV format and returns array of Strings
     * Implementation of parsing is extracted from open-csv.
     * http://opencsv.sourceforge.net/
     *
     * @param csvLine
     * @param separator
     * @param quotechar
     * @param escape
     * @param strictQuotes
     * @return
     * @throws IOException
     */
    public static List<String> parseCsvLine(String csvLine,
                                            char separator, char quotechar,
                                            char escape, boolean strictQuotes) {

        List<String>tokensOnThisLine = new ArrayList<String>();
        StringBuilder sb = new StringBuilder(50);
        boolean inQuotes = false;
        for (int i = 0; i < csvLine.length(); i++) {
            char c = csvLine.charAt(i);
            if (c == escape) {
                boolean isNextCharEscapable = inQuotes  // we are in quotes, therefore there can be escaped quotes in here.
                        && csvLine.length() > (i+1)  // there is indeed another character to check.
                        && ( csvLine.charAt(i+1) == quotechar || csvLine.charAt(i+1) == escape);

                if( isNextCharEscapable ){
                    sb.append(csvLine.charAt(i+1));
                    i++;
                }
            } else if (c == quotechar) {
                boolean isNextCharEscapedQuote = inQuotes  // we are in quotes, therefore there can be escaped quotes in here.
                        && csvLine.length() > (i+1)  // there is indeed another character to check.
                        && csvLine.charAt(i+1) == quotechar;
                if( isNextCharEscapedQuote ){
                    sb.append(csvLine.charAt(i+1));
                    i++;
                }else{
                    inQuotes = !inQuotes;
                    // the tricky case of an embedded quote in the middle: a,bc"d"ef,g
                    if (!strictQuotes) {
                        if(i>2 //not on the beginning of the line
                                && csvLine.charAt(i-1) != separator //not at the beginning of an escape sequence
                                && csvLine.length()>(i+1) &&
                                csvLine.charAt(i+1) != separator //not at the  end of an escape sequence
                                ){
                            sb.append(c);
                        }
                    }
                }
            } else if (c == separator && !inQuotes) {
                tokensOnThisLine.add(sb.toString());
                sb = new StringBuilder(50); // start work on next token
            } else {
                if (!strictQuotes || inQuotes)
                    sb.append(c);
            }
        }
        // line is done - check status
        if (inQuotes) {
            //  _log.warn("Un-terminated quoted field at end of CSV line. \n ["+csvLine+"]");
        }
        if (sb != null) {
            tokensOnThisLine.add(sb.toString());
        }
        return tokensOnThisLine;
    }


}