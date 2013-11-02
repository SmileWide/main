package smile.wide.utils;

import org.apache.commons.lang.mutable.MutableDouble;

public class SMILEMath {

	private static double LogGamma(double xx)
	{//Copied from Numerical Recipes in C, 2nd edition.

	 double x,y,tmp,ser;
	 final double cof[] = {76.18009172947146,-86.50532032941677,24.01409824083091,
	  -1.231739572450155,0.1208650973866179e-2,-0.5395239384953e-5};
	 int j;

	 y=x=xx;
	 tmp = x+5.5;
	 tmp -= (x+0.5)*Math.log(tmp);
	 ser = 1.000000000190015;
	 for (j=0; j<=5; j++) ser += cof[j]/++y;
	 return -tmp+Math.log(2.5066282746310005*ser/x);
	}
	//------------------------------------------------------
	//Returns the incomplete gamma function Q(a,x) evaluated by its continued
	//fraction representation as gammcf.  Also returns lnGamma(a) as gln.
	private static void gcf(MutableDouble gammcf, double a, double x, MutableDouble gln)
	{
	 int    ITMAX = 100;
	 double EPS   = 3.0e-7;
	 double FPMIN = 1.0e-30;
	 int i;
	 double an,b,c,d,del,h;

	 gln.setValue(LogGamma(a));
	 b=(double) (x+1.0-a);
	 c=(double) (1.0/FPMIN);
	 d=(double) (1.0/b);
	 h=d;
	 for (i=1;i<=ITMAX;i++)
	 {
	  an = -i*(i-a);
	  b+=2.0;
	  d=an*d+b;
	  if (Math.abs(d) < FPMIN) d=(double)FPMIN;
	  c=b+an/c;
	  if (Math.abs(c) < FPMIN) c=(double)FPMIN;
	  d=(double)1.0/d;
	  del=d*c;
	  h *= del;
	  if (Math.abs(del-1.0) < EPS) break;
	 }
	 if (i>ITMAX)
	 {;}//std::cout<<"a too large, ITMAX too small in gcf";}
	 gammcf.setValue(Math.exp(-x+a*Math.log(x)-(gln.doubleValue()))*h);
	}

	//------------------------------------------------------
	//Returns the incomplete gamma function P(a,x) evaluated by
	//its series representation as gamser.  Also returns ln(Gamma(a) 
	//as gln.
	private static void gser(MutableDouble gamser, double a, double x, MutableDouble gln)
	{
	 int    ITMAX = 100;
	 double EPS   = 3.0e-7;

	 int n;
	 double sum,del,ap;

	 gln.setValue(LogGamma(a));
	 if (x<=0.0)
	 {
	  //std::cout<<"x less than 0 in routine gser".;(throw exception)
	  gamser.setValue(0.0);
	  return;
	 }
	 else
	 {
	  ap=a;
	  del=sum=(double) (1.0/a);
	  for (n=1;n<=ITMAX;n++)
	  {
	   ++ap;
	   del *= x/ap;
	   sum += del;
	   if (Math.abs(del) < Math.abs(sum)*EPS)
	   {
	    gamser.setValue(sum*Math.exp(-x+a*Math.log(x)-(gln.doubleValue())));
	    return;
	   }
	  }
	  //std::cout<<"a too large, ITMAX too small in routine gser");(throw exception?)
	  return;
	 }
	}
	//------------------------------------------------------
	//the incomplete gamma function P(a,x)
	public static double gammp(double a, double x)
	{
	 MutableDouble gamser=new MutableDouble(0.0);
	 MutableDouble gammcf=new MutableDouble(0.0);
	 MutableDouble gln=new MutableDouble(0.0);

	 if (x<0.0 || a<=0.0) 
	 {
	//  std::cout<<"Bad Input values into gammp!";
	  return -1;
	 }

	 if (x < (a+1.0))
	 {
	  gser(gamser,a,x,gln);
	  return gamser.doubleValue();
	 }
	 else
	 {
	  gcf(gammcf,a,x,gln);
	  return (double) (1.0-gammcf.doubleValue());
	 }
	}
	//------------------------------------------------------
	//the incomplete gamma function Q(a,x) def= 1=P(a,x).
	public static double gammq(double a, double x)
	{
	 MutableDouble gamser=new MutableDouble(0.0);
	 MutableDouble gammcf=new MutableDouble(0.0);
	 MutableDouble gln= new MutableDouble(0.0);

	 if (x<0.0 || a<=0.0) 
	 {
	  return -1;//throw expection?
	 }
	 if (x < (a+1.0))
	 {
	  gser(gamser,a,x,gln);
	  return (double) (1.0-gamser.doubleValue());
	 }
	 else
	 {
	  gcf(gammcf,a,x,gln);
	  return (double) (gammcf.doubleValue());
	 }
	}

	/* normal cdf */
	public static double normalcdf(double z)
	{
	    if (z > 6.0)  { return 1.0; }
	    if (z < -6.0) { return 0.0; }
	    
	    double b1 =  0.31938153;
	    double b2 = -0.356563782;
	    double b3 =  1.781477937;
	    double b4 = -1.821255978;
	    double b5 =  1.330274429;
	    double p  =  0.2316419;
	    double c2 =  0.3989423;
	    
	    double a = Math.abs(z);
	    double t = 1.0/(1.0+a*p);
	    double b = c2*Math.exp((-z)*(z/2.0));
	    double n = ((((b5*t+b4)*t+b3)*t+b2)*t+b1)*t;
	    n = 1.0-b*n;
	    if (z < 0.0) n = 1.0 - n;
	    return n;
	}
}
