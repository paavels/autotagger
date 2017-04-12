# AutoTagger
This application does simplified categorization of CSV rows based on defined rules.
It is rather than *quick and dirty* implementation than complete and foolproof package. 

## Rules
Categorization rules are stored in file rules.txt

### File example
```
US Northeast Garbage Renters
	addr_state	IS ANY	PA,NY,NJ,VT,RI,NH,MA,ME,CT
	grade	IS NOT	A,B,C
	home_ownership	IS ANY	RENT,MORTGAGE
```

### File format

First row is name of rule

Followed by rows of rule definitions

Empty line defines end of rule

```
**rule**
	\t	<field_name>	\t	<keyword>	\t	<value or values>

```
Multiple values are separated by comma

## Keywords

**IS** value - verifies that field value equals to defined value

**EQUALS** value - same as above

**IS ANY** values - verifies that field value is within list of values

**IS NOT** value or values - verifies that field value is not within list of values

**ALL EXCEPT** value or values - same as above

## Performance

1. C# / Single thread / Array based run: 13.52sec
2. Python / Single thread / Array based run: 24.01
3. Python / Single thread / String based run: 24.04sec

C# probably has some issue with IS NOT, but it is definitely faster
Python is slower, however more nice to work with. Array based run showed same performance as checking parts of string

Test data used
https://www.kaggle.com/wendykan/lending-club-loan-data
