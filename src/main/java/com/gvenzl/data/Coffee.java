package com.gvenzl.data;

import java.util.Random;

public class Coffee {
	
	public Coffee() {
		initialize();
	}
	
	public class CoffeeEntry {
		public String coffee;
		public double salesAmount;
	}
	
	private static int arraySize = 24;
	private static String[][] coffees = new String[arraySize][2];
	
	private void initialize() {
		coffees[0][0] =
				"  {                                     \n"+
				"    \"item\": \"Espresso\",             \n"+
				"    \"size\": \"small\",                \n"+
				"    \"salesAmount\": 2.10,             \n"+
				"    \"coffeeUsedGram\": 10            \n"+
				"  }";
		coffees[0][1] = "2.10";

		coffees[1][0] =
				"  {                                     \n"+
				"    \"item\": \"Espresso\",             \n"+
				"    \"size\": \"medium\",               \n"+
				"    \"salesAmount\": 2.30,             \n"+
				"    \"coffeeUsedGram\": 15            \n"+
				"  }";
		coffees[1][1] = "2.30";

		coffees[2][0] =
				"  {                                     \n"+
				"    \"item\": \"Espresso\",             \n"+
				"    \"size\": \"large\",                \n"+
				"    \"salesAmount\": 2.50,             \n"+
				"    \"coffeeUsedGram\": 25            \n"+
				"  }";
		coffees[2][1] = "2.50";

		coffees[3][0] =
				"  {                                     \n"+
				"    \"item\": \"Americano\",            \n"+
				"    \"size\": \"small\",                \n"+
				"    \"salesAmount\": 2.65,             \n"+
				"    \"coffeeUsedGram\": 10            \n"+
				"  }";
		coffees[3][1] = "2.65";

		coffees[4][0] =
				"  {                                     \n"+
				"    \"item\": \"Americano\",            \n"+
				"    \"size\": \"medium\",               \n"+
				"    \"salesAmount\": 2.95,             \n"+
				"    \"coffeeUsedGram\": 15            \n"+
				"  }";
		coffees[4][1] = "2.95";

		coffees[5][0] =
				"  {                                     \n"+
				"    \"item\": \"Americano\",            \n"+
				"    \"size\": \"large\",                \n"+
				"    \"salesAmount\": 3.25,             \n"+
				"    \"coffeeUsedGram\": 25            \n"+
				"  }";
		coffees[5][1] = "3.25";

		coffees[6][0] =
				"  {                                     \n"+
				"    \"item\": \"Latte\",                \n"+
				"    \"size\": \"small\",                \n"+
				"    \"salesAmount\": 2.95,             \n"+
				"    \"coffeeUsedGram\": 10            \n"+
				"  }";
		coffees[6][1] = "2.95";

		coffees[7][0] =
				"  {                                     \n"+
				"    \"item\": \"Latte\",                \n"+
				"    \"size\": \"medium\",               \n"+
				"    \"salesAmount\": 3.65,             \n"+
				"    \"coffeeUsedGram\": 15            \n"+
				"  }";
		coffees[7][1] = "3.65";

		coffees[8][0] =
				"  {                                     \n"+
				"    \"item\": \"Latte\",                \n"+
				"    \"size\": \"large\",                \n"+
				"    \"salesAmount\": 4.15,             \n"+
				"    \"coffeeUsedGram\": 25            \n"+
				"  }";
		coffees[8][1] = "4.15";

		coffees[9][0] =
				"  {                                     \n"+
				"    \"item\": \"Cappuccino\",           \n"+
				"    \"size\": \"small\",                \n"+
				"    \"salesAmount\": 3.15,             \n"+
				"    \"coffeeUsedGram\": 10            \n"+
				"  }";
		coffees[9][1] = "3.15";

		coffees[10][0] =
				"  {                                     \n"+
				"    \"item\": \"Cappuccino\",           \n"+
				"    \"size\": \"medium\",               \n"+
				"    \"salesAmount\": 3.45,             \n"+
				"    \"coffeeUsedGram\": 15            \n"+
				"  }";
		coffees[10][1] = "3.45";

		coffees[11][0] =
				"  {                                     \n"+
				"    \"item\": \"Cappuccino\",           \n"+
				"    \"size\": \"large\",                \n"+
				"    \"salesAmount\": 3.85,             \n"+
				"    \"coffeeUsedGram\": 25            \n"+
				"  }";
		coffees[11][1] = "3.85";

		coffees[12][0] =
				"  {                                     \n"+
				"    \"item\": \"Macchiato\",            \n"+
				"    \"size\": \"small\",                \n"+
				"    \"salesAmount\": 3.75,             \n"+
				"    \"coffeeUsedGram\": 10            \n"+
				"  }";
		coffees[12][1] = "3.75";
		
		coffees[13][0] =
				"  {                                     \n"+
				"    \"item\": \"Macchiato\",            \n"+
				"    \"size\": \"medium\",               \n"+
				"    \"salesAmount\": 4.45,             \n"+
				"    \"coffeeUsedGram\": 15            \n"+
				"  }";
		coffees[13][1] = "4.45";

		coffees[14][0] =
				"  {                                     \n"+
				"    \"item\": \"Macchiato\",            \n"+
				"    \"size\": \"large\",                \n"+
				"    \"salesAmount\": 4.75,             \n"+
				"    \"coffeeUsedGram\": 25            \n"+
				"  }";
		coffees[14][1] = "4.75";

		coffees[15][0] =
				"  {                                     \n"+
				"    \"item\": \"Vanilla Cream Coffee\", \n"+
				"    \"size\": \"small\",                \n"+
				"    \"salesAmount\": 3.25,             \n"+
				"    \"coffeeUsedGram\": 10            \n"+
				"  }";
		coffees[15][1] = "3.25";

		coffees[16][0] =
				"  {                                     \n"+
				"    \"item\": \"Vanilla Cream Coffee\", \n"+
				"    \"size\": \"medium\",               \n"+
				"    \"salesAmount\": 3.95,             \n"+
				"    \"coffeeUsedGram\": 15            \n"+
				"  }";
		coffees[16][1] = "3.95";

		coffees[17][0] =
				"  {                                     \n"+
				"    \"item\": \"Vanilla Cream Coffee\", \n"+
				"    \"size\": \"large\",                \n"+
				"    \"salesAmount\": 4.45,             \n"+
				"    \"coffeeUsedGram\": 25            \n"+
				"  }";
		coffees[17][1] = "4.45";

		coffees[18][0] =
				"  {                                     \n"+
				"    \"item\": \"Caramel Cream Coffee\", \n"+
				"    \"size\": \"small\",                \n"+
				"    \"salesAmount\": 3.95,             \n"+
				"    \"coffeeUsedGram\": 10            \n"+
				"  }";
		coffees[18][1] = "3.95";
		
		coffees[19][0] =
				"  {                                     \n"+
				"    \"item\": \"Caramel Cream Coffee\", \n"+
				"    \"size\": \"medium\",               \n"+
				"    \"salesAmount\": 4.45,             \n"+
				"    \"coffeeUsedGram\": 15            \n"+
				"  }";
		coffees[19][1] = "4.45";

		coffees[20][0] =
				"  {                                     \n"+
				"    \"item\": \"Caramel Cream Coffee\", \n"+
				"    \"size\": \"large\",                \n"+
				"    \"salesAmount\": 4.95,             \n"+
				"    \"coffeeUsedGram\": 25            \n"+
				"  }";
		coffees[20][1] = "4.95";

		coffees[21][0] =
				"  {                                     \n"+
				"    \"item\": \"Mocha\",                \n"+
				"    \"size\": \"small\",                \n"+
				"    \"salesAmount\": 3.45,             \n"+
				"    \"coffeeUsedGram\": 10            \n"+
				"  }";
		coffees[21][1] = "3.45";

		coffees[22][0] =
				"  {                                     \n"+
				"    \"item\": \"Mocha\",                \n"+
				"    \"size\": \"medium\",               \n"+
				"    \"salesAmount\": 4.15,             \n"+
				"    \"coffeeUsedGram\": 15            \n"+
				"  }";
		coffees[22][1] = "4.15";

		coffees[23][0] =
				"  {                                     \n"+
				"    \"item\": \"Mocha\",                \n"+
				"    \"size\": \"large\",                \n"+
				"    \"salesAmount\": 4.65,             \n"+
				"    \"coffeeUsedGram\": 25            \n"+
				"  }";
		coffees[23][1] = "4.65";
	}
	
	public CoffeeEntry getCoffee() {
		
		CoffeeEntry ret = new CoffeeEntry();
		int entry = new Random().nextInt(arraySize);
		ret.coffee =       coffees[entry][0];
		ret.salesAmount = Double.valueOf(coffees[entry][1]);

		return ret;
	}
}
