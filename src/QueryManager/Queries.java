package QueryManager;

public class Queries
{
    public static final String CREATE_TABLE1   = "Create table Details (Name,Surname,Age) primary-key Name";
    public static final String INSERT_TABLE1  = "Insert into Details (Name,Surname,Age) values (Ayushi,Singh,29)";

    public static final String CREATE_TABLE2   = "Create table Cartoons (Name,Character) primary-key Name";
    public static final String INSERT_TABLE2   = "Insert into Cartoons (Name,Character) values (Ayushi,Lala)";

    public static final String UPDATE_TABLE   = "Update table Details (Surname) value (Singgh) where Name = Ayushi";
    public static final String DELETE_TABLE   = "Delete from Details (Surname) where Name = Ayushi";
    public static final String READ_TABLE     = "Select from Details (Age) where Name = Ayushi";
    public static final String READ_ALL_TABLE = "Select from Details *";
    public static final String JOIN_TABLE     = "Select from Details (Surname,Age) AND from Cartoons (Character) where Details.Name = Cartoons.Name";
}
