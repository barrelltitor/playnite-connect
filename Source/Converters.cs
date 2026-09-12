using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows.Markup;

namespace MQTTClient
{
    public class NullableIntToStringConverter : MarkupExtension, IValueConverter
    {
        public override object ProvideValue(IServiceProvider serviceProvider)
        {
            return this;
        }

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            return value is int number ? number.ToString(culture) : string.Empty;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            var text = value as string;
            if (string.IsNullOrWhiteSpace(text))
            {
                return null;
            }

            return int.TryParse(text, NumberStyles.Integer, culture, out var number)
                ? (object)number
                : Binding.DoNothing;
        }
    }
}