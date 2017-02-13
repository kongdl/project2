package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MyFakebookOracle extends FakebookOracle {

    static String prefix = "syzhao.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding tables in your database
    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;


    // DO NOT modify this constructor
    public MyFakebookOracle(String dataType, Connection c) {
        super();
        oracleConnection = c;
        // You will use the following tables in your Java code
        cityTableName = prefix + dataType + "_CITIES";
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITY";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITY";
        programTableName = prefix + dataType + "_PROGRAMS";
        educationTableName = prefix + dataType + "_EDUCATION";
        eventTableName = prefix + dataType + "_USER_EVENTS";
        albumTableName = prefix + dataType + "_ALBUMS";
        photoTableName = prefix + dataType + "_PHOTOS";
        tagTableName = prefix + dataType + "_TAGS";
    }


    @Override
    // ***** Query 0 *****
    // This query is given to your for free;
    // You can use it as an example to help you write your own code
    //
    public void findMonthOfBirthInfo() {

        // Scrollable result set allows us to read forward (using next())
        // and also backward.
        // This is needed here to support the user of isFirst() and isLast() methods,
        // but in many cases you will not need it.
        // To create a "normal" (unscrollable) statement, you would simply call
        // Statement stmt = oracleConnection.createStatement();
        //
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

            // For each month, find the number of users born that month
            // Sort them in descending order of count
            ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from " +
                    userTableName +
                    " where month_of_birth is not null group by month_of_birth order by 1 desc");

            this.monthOfMostUsers = 0;
            this.monthOfLeastUsers = 0;
            this.totalUsersWithMonthOfBirth = 0;

            // Get the month with most users, and the month with least users.
            // (Notice that this only considers months for which the number of users is > 0)
            // Also, count how many total users have listed month of birth (i.e., month_of_birth not null)
            //
            while (rst.next()) {
                int count = rst.getInt(1);
                int month = rst.getInt(2);
                if (rst.isFirst())
                    this.monthOfMostUsers = month;
                if (rst.isLast())
                    this.monthOfLeastUsers = month;
                this.totalUsersWithMonthOfBirth += count;
            }

            // Get the names of users born in the "most" month
            rst = stmt.executeQuery("select user_id, first_name, last_name from " +
                    userTableName + " where month_of_birth=" + this.monthOfMostUsers);
            while (rst.next()) {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                this.usersInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
            }

            // Get the names of users born in the "least" month
            rst = stmt.executeQuery("select first_name, last_name, user_id from " +
                    userTableName + " where month_of_birth=" + this.monthOfLeastUsers);
            while (rst.next()) {
                String firstName = rst.getString(1);
                String lastName = rst.getString(2);
                Long uid = rst.getLong(3);
                this.usersInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
            }

            // Close statement and result set
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 1 *****
    // Find information about users' names:
    // (1) The longest first name (if there is a tie, include all in result)
    // (2) The shortest first name (if there is a tie, include all in result)
    // (3) The most common first name, and the number of times it appears (if there
    //      is a tie, include all in result)
    //
    public void findNameInfo() { // Query1
        // Find the following information from your database and store the information as shown
        try (Statement stmt=
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)){
            //get longest and shortest firstname

            ResultSet rst=stmt.executeQuery("select first_name, LENGTH(first_name) from "+userTableName+
                " order by LENGTH(first_name)");

            int longest = 0;
            int shortest=0;
            while(rst.next()){
                if (rst.isFirst())  shortest=rst.getInt(2);
                if (rst.isLast())   longest=rst.getInt(2);
            }


            rst=stmt.executeQuery("select first_name, LENGTH(first_name) from "+userTableName+
                " where LENGTH(first_name)="+shortest+" or LENGTH(first_name) ="+longest+" order by LENGTH(first_name)");
            while(rst.next()){
                String temp=rst.getString(1);
                if(rst.getInt(2)==shortest) this.shortestFirstNames.add(new String(temp));
                else    this.longestFirstNames.add(new String(temp));
            }

            //get most common first name
            rst=stmt.executeQuery("select first_name, count(*) from "+userTableName+
                " group by first_name order by count(*) desc");
            int count=0;
            while(rst.next()){
                if(rst.isFirst()){
                    count=rst.getInt(2);
                    this.mostCommonFirstNamesCount=count;
                }
                if(rst.getInt(2)==count)    this.mostCommonFirstNames.add(new String(rst.getString(1)));
                else    break;
            }
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 2 *****
    // Find the user(s) who have no friends in the network
    //
    // Be careful on this query!
    // Remember that if two users are friends, the friends table
    // only contains the pair of user ids once, subject to
    // the constraint that user1_id < user2_id
    //
    public void lonelyUsers() {
        // Find the following information from your database and store the information as shown
        this.lonelyUsers.add(new UserInfo(10L, "Billy", "SmellsFunny"));
        this.lonelyUsers.add(new UserInfo(11L, "Jenny", "BadBreath"));
    }

    @Override
    // ***** Query 3 *****
    // Find the users who do not live in their hometowns
    // (I.e., current_city != hometown_city)
    //
    public void liveAwayFromHome() throws SQLException {
        try (Statement stmt=
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)){
            //get longest and shortest firstname

            ResultSet rst=stmt.executeQuery("select U.user_id, U.first_name, U.last_name from "+userTableName+" U, "+currentCityTableName+
                " C, "+hometownCityTableName+" H "+" where U.user_id=C.user_id and U.user_id=H.user_id and C.current_city_id <> H.hometown_city_id");

            
            while(rst.next()){
                Long userId=rst.getLong(1);
                String firstName=rst.getString(2);
                String lastName=rst.getString(3); 
                this.liveAwayFromHome.add(new UserInfo(userId, firstName, lastName));
            }
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // **** Query 4 ****
    // Find the top-n photos based on the number of tagged users
    // If there are ties, choose the photo with the smaller numeric PhotoID first
    //
    public void findPhotosWithMostTags(int n) {
        String photoId = "1234567";
        String albumId = "123456789";
        String albumName = "album1";
        String photoCaption = "caption1";
        String photoLink = "http://google.com";
        PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
        TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
        tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName1", "taggedUserLastName1"));
        tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName2", "taggedUserLastName2"));
        this.photosWithMostTags.add(tp);
    }

    @Override
    // **** Query 5 ****
    // Find suggested "match pairs" of users, using the following criteria:
    // (1) Both users should be of the same gender
    // (2) They should be tagged together in at least one photo (They do not have to be friends of the same person)
    // (3) Their age difference is <= yearDiff (just compare the years of birth for this)
    // (4) They are not friends with one another
    //
    // You should return up to n "match pairs"
    // If there are more than n match pairs, you should break ties as follows:
    // (i) First choose the pairs with the largest number of shared photos
    // (ii) If there are still ties, choose the pair with the smaller user1_id
    // (iii) If there are still ties, choose the pair with the smaller user2_id
    //
    public void matchMaker(int n, int yearDiff) {
        try (Statement stmt=
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)){
            //get longest and shortest firstname

            ResultSet rst=stmt.executeQuery("select distinct U1.user_id, U2.user_id, COUNT(T1.tag_photo_id) from "+userTableName+" U1, "+userTableName+" U2, "
                +tagTableName+" T1, "+tagTableName+" T2 "+
                " WHERE U1.user_id=T1.tag_subject_id AND U2.user_id=T2.tag_subject_id AND T1.tag_photo_id=T2.tag_photo_id AND U1.gender=U2.gender AND U1.user_id<U2.user_id AND ABS(U1.YEAR_OF_BIRTH-U2.YEAR_OF_BIRTH)<"+yearDiff
                +" AND NOT EXISTS (SELECT USER1_ID, USER2_ID FROM " + friendsTableName+
                " F WHERE (U1.USER_ID=F.USER1_ID AND U2.USER_ID=F.USER2_ID) OR (U1.USER_ID=F.USER2_ID AND U2.USER_ID=F.USER1_ID)) GROUP BY U1.USER_ID, U2.USER_ID ORDER BY U1.USER_ID, COUNT(T1.TAG_PHOTO_ID) DESC");

            int count=0;
            while(rst.next()&&count<n){
                Long u1UserId=rst.getLong(1);
                Long u2UserId=rst.getLong(2);
                System.out.println("User1: "+u1UserId+" User2: "+u2UserId);

                try(Statement stmt2=
                        oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_READ_ONLY)){
                    ResultSet rst2=stmt2.executeQuery("SELECT U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH FROM "
                        +userTableName+" U1, "+userTableName+" U2 "+ " WHERE U1.USER_ID="+u1UserId+" AND U2.USER_ID="+u2UserId);
                    while(rst2.next()){
                        String u1FirstName = rst2.getString(1);
                        String u1LastName = rst2.getString(2);
                        int u1Year=rst2.getInt(3);
                        String u2FirstName = rst2.getString(4);
                        String u2LastName = rst2.getString(5);
                        int u2Year=rst2.getInt(6);
                        MatchPair mp = new MatchPair(u1UserId, u1FirstName, u1LastName,
                            u1Year, u2UserId, u2FirstName, u2LastName, u2Year);

                        try(Statement stmt3=
                            oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_READ_ONLY)){

                            ResultSet rst3=stmt3.executeQuery("SELECT P.PHOTO_ID, P.ALBUM_ID, A.ALBUM_NAME, P.PHOTO_CAPTION, P.PHOTO_LINK FROM "+userTableName+" U1, "
                                +userTableName+" U2, "+tagTableName+" T1, "+tagTableName+" T2, "+photoTableName+" P, "+ albumTableName+" A "+
                                " WHERE U1.USER_ID="+u1UserId+" AND U2.USER_ID="+u2UserId+
                                " AND U1.user_id=T1.tag_subject_id AND U2.user_id=T2.tag_subject_id AND T1.tag_photo_id=T2.tag_photo_id AND P.PHOTO_ID=T1.TAG_PHOTO_ID AND P.ALBUM_ID=A.ALBUM_ID");
                            while(rst3.next()){
                                String sharedPhotoId = rst3.getString(1);
                                String sharedPhotoAlbumId = rst3.getString(2);
                                String sharedPhotoAlbumName = rst3.getString(3);
                                String sharedPhotoCaption = rst3.getString(4);
                                String sharedPhotoLink = rst3.getString(5);
                                mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId,
                                    sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
                            }

                            rst3.close();
                            stmt3.close();
                        }catch (SQLException err) {
                            System.err.println(err.getMessage());
                            }
                        this.bestMatches.add(mp);
                    }
                    rst2.close();
                    stmt2.close();
                } catch (SQLException err) {
                    System.err.println(err.getMessage());
                    }
            count++;
            }
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    // **** Query 6 ****
    // Suggest users based on mutual friends
    //
    // Find the top n pairs of users in the database who have the most
    // common friends, but are not friends themselves.
    //
    // Your output will consist of a set of pairs (user1_id, user2_id)
    // No pair should appear in the result twice; you should always order the pairs so that
    // user1_id < user2_id
    //
    // If there are ties, you should give priority to the pair with the smaller user1_id.
    // If there are still ties, give priority to the pair with the smaller user2_id.
    //
    @Override
    public void suggestFriendsByMutualFriends(int n) {
        try (Statement stmt=
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)){
            //get longest and shortest firstname

            ResultSet rst=stmt.executeQuery("SELECT * FROM (select U1.user_id, U2.user_id, count(*) from "+userTableName+" U1, "+userTableName+" U2, "+userTableName+" U3, "++userTableName+" U4 "
                "where U1.USER_ID < U2.USER_ID AND U3.USER_ID = U4.USER_ID AND EXISTS (SELECT * FROM "+friendsTableName+
                " F WHERE (U1.USER_ID=F.USER1_ID AND U3.USER_ID=F.USER2_ID) OR (U3.USER_ID=F.USER1_ID AND U1.USER_ID=F.USER2_ID)) "+
                " AND EXISTS (SELECT * FROM "+friendsTableName+
                " F WHERE (U2.USER_ID=F.USER1_ID AND U4.USER_ID=F.USER2_ID) OR (U4.USER_ID=F.USER1_ID AND U2.USER_ID=F.USER2_ID)) GROUP BY U1.USER_ID, U2.USER_ID ORDER BY COUNT(*) DESC, U1.USER_ID, U2.USER_ID) WHERE ROWNUM <= "+n);

            while(rst.next()){
                Long user1_Id=rst.getLong(1);
                Long user2_Id=rst.getLong(2);
                Statement stmt2 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet rst2 = stmt2.executeQuery("select U1.FIRST_NAME, U1.LAST_NAME, U2.FIRST_NAME, U2.LAST_NAME FROM "+userTableName+" U1, "+userTableName+" U2 "+
                    "WHERE U1.USER_ID = "+user1_Id+" AND U2.USER_ID = "+ user2_Id);
                while(rst2.next()){
                    String user1FirstName = rst2.getString(1);
                    String user1LastName = rst2.getString(2);
                    String user2FirstName = rst2.getString(3);
                    String user2LastName = rst2.getString(4);
                    UsersPair p = new UsersPair(user1_Id, user1FirstName, user1LastName, user2_Id, user2FirstName, user2LastName);
                    Statement stmt3 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    ResultSet rst3 = stmt3.executeQuery("select U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME FROM "+userTableName+" U1, "+userTableName+" U2, "+userTableName+" U3 "+
                        "WHERE U2.USER_ID="+user1_Id+" AND U3.USER_ID="+user2_Id+" AND EXISTS (SELECT * FROM "+friendsTableName+
                    " F WHERE (U1.USER_ID=F.USER1_ID AND U2.USER_ID=F.USER2_ID) OR (U2.USER_ID=F.USER1_ID AND U1.USER_ID=F.USER2_ID)) "+" AND EXISTS (SELECT * FROM "+friendsTableName+
                    " F WHERE (U1.USER_ID=F.USER1_ID AND U3.USER_ID=F.USER2_ID) OR (U3.USER_ID=F.USER1_ID AND U1.USER_ID=F.USER2_ID)) ");
                    while(rst3.next()){
                        Long shared_ID = rst3.getLong(1);
                        String shared_first = rst3.getString(2);
                        String shared_last = rst3.getString(3);
                        p.addSharedFriend(shared_ID, shared_first, shared_last);
                    }
                }
                this.suggestedUsersPairs.add(p);
            }
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 7 *****
    //
    // Find the name of the state with the most events, as well as the number of
    // events in that state.  If there is a tie, return the names of all of the (tied) states.
    //
    public void findEventStates() {
        try (Statement stmt=
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)){
            //get longest and shortest firstname

            ResultSet rst=stmt.executeQuery("SELECT C.STATE_NAME, COUNT(*) FROM "+eventTableName+" E, "+cityTableName+" C "+
            "WHERE E.EVENT_CITY_ID=C.CITY_ID GROUP BY C.STATE_NAME ORDER BY COUNT(*) DESC" );

            int max=0;
            while(rst.next()){
                if(rst.isFirst()){
                    max=rst.getInt(2);
                    this.eventCount = max;
                }
                if(rst.getInt(2)==max){
                    this.popularStateNames.add(rst.getString(1));
                }
            }
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
        /*
        this.eventCount = 12;
        this.popularStateNames.add("Michigan");
        this.popularStateNames.add("California");
        */
    }

    //@Override
    // ***** Query 8 *****
    // Given the ID of a user, find information about that
    // user's oldest friend and youngest friend
    //
    // If two users have exactly the same age, meaning that they were born
    // on the same day, then assume that the one with the larger user_id is older
    //
    public void findAgeInfo(Long user_id) {
        this.oldestFriend = new UserInfo(1L, "Oliver", "Oldham");
        this.youngestFriend = new UserInfo(25L, "Yolanda", "Young");
    }

    @Override
    //	 ***** Query 9 *****
    //
    // Find pairs of potential siblings.
    //
    // A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
    // if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
    // on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
    //
    //
    public void findPotentialSiblings() {
        try (Statement stmt=
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)){
            //get longest and shortest firstname

            ResultSet rst=stmt.executeQuery("SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME FROM "+
            userTableName+" U1, "+ userTableName+" U2, "+hometownCityTableName+" H1, "+hometownCityTableName+" H2 "+
            " WHERE U1.LAST_NAME=U2.LAST_NAME AND U1.USER_ID<U2.USER_ID AND ABS(U1.YEAR_OF_BIRTH-U2.YEAR_OF_BIRTH)<10"+
            " AND U1.USER_ID=H1.USER_ID AND U2.USER_ID=H2.USER_ID AND H1.HOMETOWN_CITY_ID=H2.HOMETOWN_CITY_ID "+
            "AND EXISTS (SELECT USER1_ID, USER2_ID FROM "+friendsTableName+" F WHERE U1.USER_ID=F.USER1_ID AND U2.USER_ID=F.USER2_ID) "+
            " ORDER BY U1.USER_ID ASC, U2.USER_ID ASC");

            while(rst.next()){
                Long user1_id = rst.getLong(1);
                String user1FirstName = rst.getString(2);
                String user1LastName = rst.getString(3);
                Long user2_id = rst.getLong(4);
                String user2FirstName = rst.getString(5);
                String user2LastName = rst.getString(6);
                SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
                this.siblings.add(s);
            }
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
        /*
        Long user1_id = 123L;
        String user1FirstName = "User1FirstName";
        String user1LastName = "User1LastName";
        Long user2_id = 456L;
        String user2FirstName = "User2FirstName";
        String user2LastName = "User2LastName";
        SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
        this.siblings.add(s);
        */
    }

}
